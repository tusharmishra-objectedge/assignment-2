from connector import engine
from sqlalchemy import text
from sqlalchemy.orm import Session


def create_table(name, columns):
    try:
        columns = ", ".join(f"{col} {typ}" for col, typ in columns.items())
        stmt = text(f"CREATE TABLE {name} ({columns})")
        with Session(engine) as session:
            session.execute(stmt)
            session.commit()
    except Exception as e:
        print(f"failed due to {e}")


customer_partner_relation_table = {
    "customer_status": "text",
    "partner_status": "text",
    "cpqmodel": "text",
    "customer_account_type": "text",
    "customer_sfdc_account_id": "text",
    "customer_woc_ref": "bigint",
    "package": "text",
    "partner_sfdc_account_id": "text",
    "total_qty": "int",
    "subscription_lines": "jsonb[]",
    "entities": "jsonb[]",
}
create_table("customer_partner_relation_table", customer_partner_relation_table)

subscription_table = {
    "id": "serial",
    "qty": "int",
    "cpqmodel": "text",
    "customer_sfdc_account_id": "text",
    "allocated_qty": "int",
    "subscription_line_id": "bigint",
}
create_table("subscription_table", subscription_table)
