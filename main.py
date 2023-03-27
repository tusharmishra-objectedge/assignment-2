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



##
##def create_table(name, columns):
##    try:
##        columns = ", ".join(f"{col} {typ}" for col, typ in columns.items())
##        stmt = text(f"CREATE TABLE IF NOT EXISTS {name} ({columns})")
##        return execute_query(stmt)
##    except Exception as e:
##        print(f"create_table failed due to {e}")
##
##
##customer_partner_relation_table = {
##    "id": "serial PRIMARY KEY",
##    "customer_status": "text",
##    "partner_status": "text",
##    "cpqmodel": "text",
##    "customer_account_type": "text",
##    "customer_sfdc_account_id": "text",
##    "customer_woc_ref": "bigint",
##    "package": "text",
##    "partner_sfdc_account_id": "text",
##    "total_qty": "int",
##    "subscription_lines": "jsonb",
##    "entities": "jsonb",
##}
##create_table("customer_partner_relation_table", customer_partner_relation_table)
##
##subscription_table = {
##    "id": "serial",
##    "qty": "int",
##    "cpqmodel": "text",
##    "package": "text",
##    "customer_sfdc_account_id": "text",
##    "allocated_qty": "int",
##    "subscription_line_id": "bigint",
##}
##create_table("subscription_table", subscription_table)
##
##
##def import_csv_data(table, file, delimiter):
##    stmt = text(f"COPY {table} FROM '{file}' DELIMITER '{delimiter}' CSV HEADER")
##    return execute_query(stmt)
##
##
##import_csv_data(
##    "customer_partner_relation_table(customer_status,partner_status,cpqmodel,customer_account_type,customer_sfdc_account_id,customer_woc_ref,package,partner_sfdc_account_id,total_qty,subscription_lines)",
##    "D:/Python Assignments/assignment-2/output.csv",
##    "|",
##)
##
##
##
##def set_to_zero():
##    stmt = text("""
##        UPDATE customer_partner_relation_table SET subscription_lines =
##        ( SELECT jsonb_agg( jsonb_set(element, '{allocated_qty}', '0', false) )
##        FROM jsonb_array_elements(subscription_lines) AS element );
##    """)
##    return execute_query(stmt)
##
##
##set_to_zero()




class PrimaryTable(Base):
    __table__ = Base.metadata.tables["customer_partner_relation_table"]


session = Session(engine)
def get_data(parms=None):
    stmt = text('SELECT subscription_lines FROM customer_partner_relation_table ORDER BY id')
    subscription_lines = execute_query(stmt, params=None, fetch=True)
    stmt = text('SELECT total_qty FROM customer_partner_relation_table ORDER BY id')
    total_qty = execute_query(stmt, params=None, fetch=True)
    return subscription_lines, total_qty

subscription_lines, total_qty = get_data()

for tot_qty, tuple_data in zip(total_qty, subscription_lines):
    rem_qty = tot_qty[0]
    list_data = tuple_data[0]
    for jsonb_data in list_data:
        jsonb_data["allocated_qty"] = 0
    for jsonb_data in list_data:
        if rem_qty >= jsonb_data["qty"]:
            jsonb_data["allocated_qty"] = jsonb_data["qty"]
            rem_qty -= jsonb_data["qty"]
        else:
            jsonb_data["allocated_qty"] = rem_qty
            rem_qty = 0

for i, row in enumerate(subscription_lines):
    data = session.get(PrimaryTable, i + 1)
    data.subscription_lines = row[0]
session.commit()
session.close()
