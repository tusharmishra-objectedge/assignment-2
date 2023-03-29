import click
from connector import engine
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import exc
import logging


logging.basicConfig(level=logging.INFO)


def execute_query(query, params=None, fetch=False):
    result = None
    try:
        session = Session(engine)
        result = session.execute(query, params)
        if fetch:
            result = result.fetchall()
        else:
            session.commit()
    except exc.DBAPIError:
        logging.exception(
            f"Failed to execute query {query} with params {params} due to DBAPIError"
        )
        session.rollback()
    except exc.CompileError:
        logging.exception(
            f"Failed to execute query {query} with params {params} due to CompileError"
        )
        session.rollback()
    except Exception as e:
        logging.exception(
            f"Failed to execute query {query} with params {params} due to {e}"
        )
        session.rollback()
    finally:
        session.close()

    return result


def fill_subscription_table():
    stmt = text(
        """INSERT INTO subscription_table (qty, cpqmodel, package, customer_sfdc_account_id, allocated_qty, subscription_line_id) SELECT
total_qty, cpqmodel, package, customer_sfdc_account_id,
(subscription_lines_array->>'allocated_qty')::int AS allocated_qty,
(subscription_lines_array->>'subscription_line_id')::bigint AS subscription_line_id
FROM customer_partner_relation_table, jsonb_array_elements(subscription_lines) AS subscription_lines_array
WHERE customer_status = 'READY';"""
    )
    execute_query(stmt)
    logging.info("Added data to subscription table!")


def processing(params=None):
    stmt = """CREATE OR REPLACE FUNCTION process_data()
RETURNS VOID AS
$$
DECLARE
    row record;
    jsonb_data record;
    cust TEXT = '';

BEGIN
    SELECT customer_sfdc_account_id INTO cust FROM customer_partner_relation_table WHERE 1=1 """
    if params:
        if params["customer"]:
            stmt += f"AND customer_sfdc_account_id IN :customer "
        if params["partner"]:
            stmt += f"AND partner_sfdc_account_id IN :partner "
    stmt += """ORDER BY partner_sfdc_account_id, customer_sfdc_account_id LIMIT 1;
    FOR row IN SELECT id, total_qty, subscription_lines, customer_sfdc_account_id  FROM customer_partner_relation_table WHERE 1=1 """
    if params:
        if params["customer"]:
            stmt += f"AND customer_sfdc_account_id IN :customer "
        if params["partner"]:
            stmt += f"AND partner_sfdc_account_id IN :partner "
    stmt += """ORDER BY partner_sfdc_account_id, customer_sfdc_account_id
    LOOP
    BEGIN
        UPDATE customer_partner_relation_table
        SET customer_status = 'IN_PROGRESS',
            partner_status = 'IN_PROGRESS'
        WHERE id = row.id;

        FOR jsonb_data IN SELECT (row.subscription_lines ->> i)::JSONB AS data, i FROM generate_series(0, jsonb_array_length(row.subscription_lines) - 1) AS i
        LOOP
            IF row.total_qty >= (jsonb_data.data ->> 'qty')::INT THEN
                jsonb_data.data = jsonb_set(jsonb_data.data, '{allocated_qty}', to_jsonb((jsonb_data.data ->> 'qty')::INT));
                row.total_qty := row.total_qty - (jsonb_data.data ->> 'qty')::INT;
            ELSE
                jsonb_data.data = jsonb_set(jsonb_data.data, '{allocated_qty}', to_jsonb(row.total_qty));
                row.total_qty := 0;
            END IF;
        row.subscription_lines[jsonb_data.i] = jsonb_data.data;
        END LOOP;

        UPDATE customer_partner_relation_table
        SET subscription_lines = row.subscription_lines
        WHERE id = row.id;

        UPDATE customer_partner_relation_table
        SET customer_status = 'READY'
        WHERE id = row.id;

        IF row.customer_sfdc_account_id != cust THEN
            UPDATE customer_partner_relation_table
            SET partner_status = 'READY'
            WHERE customer_sfdc_account_id = cust;
            cust := row.customer_sfdc_account_id;
        END IF;
        EXCEPTION WHEN OTHERS THEN
            UPDATE customer_partner_relation_table
            SET customer_status = 'ERROR'
            WHERE customer_sfdc_account_id = cust;
        END;

    END LOOP;"""
    if not params:
        stmt += """UPDATE customer_partner_relation_table
            SET partner_status = 'READY'
            WHERE customer_sfdc_account_id = cust;"""
    stmt += """
    END;
    $$
    LANGUAGE plpgsql;"""

    execute_query(text(stmt), params)
    logging.info("created the update function!")
    exe = text("SELECT process_data();")
    execute_query(exe)
    logging.info("Updated data!")


if __name__ == "__main__":

    @click.command()
    @click.option("--customer", "-c", multiple=True, default=None)
    @click.option("--partner", "-p", multiple=True, default=None)
    def runner(customer, partner):
        data_params = {"customer": customer, "partner": partner}

        if not data_params["customer"] and not data_params["partner"]:
            data_params = None

        processing(data_params)
        fill_subscription_table()

    runner()
