import click
from connector import engine
from sqlalchemy import text
from sqlalchemy import Table
from sqlalchemy.orm import Session
from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase
import logging


logging.basicConfig(level=logging.INFO)


class Base(DeclarativeBase):
    pass


Base.metadata.reflect(engine)


class PrimaryTable(Base):
    __table__ = Base.metadata.tables["customer_partner_relation_table"]


def execute_query(query, params=None, fetch=False):
    result = None
    try:
        session = Session(engine)
        result = session.execute(query, params)
        if fetch:
            result = result.fetchall()
        else:
            session.commit()
    except Exception as e:
        logging.exception(
            f"Failed to execute query {query} with params {params} due to {e}"
        )
        session.rollback()
    finally:
        session.close()

    return result


def create_table(name, columns):
    columns = ", ".join(f"{col} {typ}" for col, typ in columns.items())
    stmt = text(f"CREATE TABLE IF NOT EXISTS {name} ({columns})")
    return execute_query(stmt)


def create_views():
    stmt = text(
        """CREATE OR REPLACE VIEW v1 AS
SELECT partner_sfdc_account_id as partner, COUNT(customer_sfdc_account_id) as no_of_customer
from customer_partner_relation_table group by partner;"""
    )
    execute_query(stmt)

    stmt = text(
        """CREATE OR REPLACE VIEW v2 AS
SELECT customer_sfdc_account_id as customer, SUM((subscription_line->>'allocated_qty')::int) as total_allocated_qty
FROM customer_partner_relation_table, jsonb_array_elements(subscription_lines) as subscription_line
GROUP BY customer;"""
    )
    execute_query(stmt)

    stmt = text(
        """CREATE OR REPLACE VIEW v3 AS
SELECT partner_sfdc_account_id as partner, SUM((subscription_line->>'allocated_qty')::int) as total_allocated_qty
FROM customer_partner_relation_table, jsonb_array_elements(subscription_lines) as subscription_line
GROUP BY partner;"""
    )
    execute_query(stmt)


def get_view(name):
    stmt = f"SELECT * FROM {name};"
    execute_query(stmt, fetch=True)


def import_csv_data(table, file, delimiter):
    stmt = text(f"COPY {table} FROM '{file}' DELIMITER '{delimiter}' CSV HEADER")
    return execute_query(stmt)


def get_data(params=None):
    stmt = "SELECT id, total_qty, subscription_lines, customer_sfdc_account_id FROM customer_partner_relation_table WHERE 1=1 "
    if params:
        if params["customer"]:
            stmt += f" AND customer_sfdc_account_id IN :customer "
        if params["partner"]:
            stmt += f"AND partner_sfdc_account_id IN :partner "
    stmt += "ORDER BY partner_sfdc_account_id, customer_sfdc_account_id"
    return execute_query(text(stmt), params, fetch=True)


def logic(qty_and_lines):
    for tot_qty, list_data in qty_and_lines:
        rem_qty = tot_qty
        for jsonb_data in list_data:
            jsonb_data["allocated_qty"] = 0
        for jsonb_data in list_data:
            if rem_qty >= jsonb_data["qty"]:
                jsonb_data["allocated_qty"] = jsonb_data["qty"]
                rem_qty -= jsonb_data["qty"]
            else:
                jsonb_data["allocated_qty"] = rem_qty
                rem_qty = 0


def process(data):
    try:
        session = Session(engine)
        cust = data[0][3]

        for row in data:
            datum = session.get(PrimaryTable, row[0])
            datum.customer_status = "IN_PROGRESS"
            datum.partner_status = "IN_PROGRESS"
            datum.subscription_lines = row[2]
            datum.customer_status = "READY"
            if row[3] != cust:
                stmt = text(
                    f"UPDATE customer_partner_relation_table SET partner_status = 'READY' WHERE customer_sfdc_account_id = '{cust}';"
                )
                session.execute(stmt)
                cust = row[3]

        session.commit()
    except Exception as e:
        logging.exception(f"Process failed due to - {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":

    @click.command()
    @click.option("--customer", "-c", multiple=True, default=None)
    @click.option("--partner", "-p", multiple=True, default=None)
    def runner(customer, partner):
        data_params = {"customer": customer, "partner": partner}

        if not data_params["customer"] and not data_params["partner"]:
            data_params = None

        customer_partner_relation_table = {
            "id": "serial PRIMARY KEY",
            "customer_status": "text",
            "partner_status": "text",
            "cpqmodel": "text",
            "customer_account_type": "text",
            "customer_sfdc_account_id": "text",
            "customer_woc_ref": "bigint",
            "package": "text",
            "partner_sfdc_account_id": "text",
            "total_qty": "int",
            "subscription_lines": "jsonb",
            "entities": "jsonb",
        }
        create_table("customer_partner_relation_table", customer_partner_relation_table)

        subscription_table = {
            "id": "serial",
            "qty": "int",
            "cpqmodel": "text",
            "package": "text",
            "customer_sfdc_account_id": "text",
            "allocated_qty": "int",
            "subscription_line_id": "bigint",
        }
        create_table("subscription_table", subscription_table)

        import_csv_data(
            "customer_partner_relation_table(customer_status,partner_status,cpqmodel,customer_account_type,customer_sfdc_account_id,customer_woc_ref,package,partner_sfdc_account_id,total_qty,subscription_lines)",
            "D:/Python Assignments/assignment-2/output.csv",
            "|",
        )

        data = get_data(data_params)
        qty_and_lines = [(t[1], t[2]) for t in data]
        logic(qty_and_lines)
        process(data)

        create_views()

    runner()
