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


def import_csv_data(table, file, delimiter):
    stmt = text(f"COPY {table} FROM '{file}' DELIMITER '{delimiter}' CSV HEADER")
    return execute_query(stmt)


def get_data(parms=None):
    stmt = text(
        """
        SELECT id, total_qty, subscription_lines, customer_sfdc_account_id, partner_sfdc_account_id
        FROM customer_partner_relation_table
        ORDER BY partner_sfdc_account_id, customer_sfdc_account_id
        """
    )
    return execute_query(stmt, params=None, fetch=True)


def logic():
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


def process():
    try:
        cust = data[0][3]
        session = Session(engine)

        for row in data:
            datum = session.get(PrimaryTable, row[0])
            datum.customer_status = "IN_PROGRESS"
            datum.partner_status = "IN_PROGRESS"
            datum.subscription_lines = row[2]
            datum.customer_status = "READY"
            if row[3] != cust:
                datum.partner_status = "READY"
                cust = row[3]

        session.commit()
    except Exception as e:
        logging.exception(f"Process failed due to - {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
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

    data = get_data()

    qty_and_lines = [(t[1], t[2]) for t in data]
    logic()
    process()
