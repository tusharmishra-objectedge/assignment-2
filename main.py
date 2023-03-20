import click
import configparser
import logging
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read("config.ini")

host = config["DEFAULT"]["host"]
port = config["DEFAULT"]["port"]
username = config["database"]["username"]
password = config["database"]["password"]
database_name = config["database"]["database_name"]

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
)
Session = sessionmaker(bind=engine)

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customer"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    dob = Column(Date)
    address = Column(String)

    def __repr__(self):
        return f"<Customer(first_name={self.first_name}, last_name={self.last_name}, dob={self.dob}, address={self.address})>"


def execute_query(query, params=None, fetch=False):
    result = None

    try:
        session = Session()
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


def c(first_name, last_name, dob, address):
    customer = Customer(
        first_name=first_name, last_name=last_name, dob=dob, address=address
    )
    session = Session()
    session.add(customer)
    session.commit()
    session.close()


def r(first_name=None, last_name=None, dob=None, address=None):
    query = "SELECT * FROM customer WHERE 1=1"
    params = {}
    if first_name:
        query += " AND first_name = :first_name"
        params["first_name"] = first_name
    if last_name:
        query += " AND last_name = :last_name"
        params["last_name"] = last_name
    if dob:
        query += " AND dob = :dob"
        params["dob"] = dob
    if address:
        query += " AND address = :address"
        params["address"] = address
    return execute_query(query, params=params, fetch=True)


def u(first_name, last_name, dob, address):
    query = "UPDATE customer SET dob=:dob, address=:address WHERE first_name=:first_name AND last_name=:last_name"
    params = {
        "dob": dob,
        "address": address,
        "first_name": first_name,
        "last_name": last_name,
    }
    execute_query(query, params=params)


def d(first_name=None, last_name=None, dob=None, address=None):
    query = "DELETE FROM customer WHERE 1=1"
    params = {}
    if first_name:
        query += " AND first_name = :first_name"
        params["first_name"] = first_name
    if last_name:
        query += " AND last_name = :last_name"
        params["last_name"] = last_name
    if dob:
        query += " AND dob = :dob"
        params["dob"] = dob
    if address:
        query += " AND address = :address"
        params["address"] = address
    execute_query(query, params=params)


if __name__ == "__main__":
    mapping = {"c": c, "r": r, "u": u, "d": d}

    @click.command()
    @click.option("--fun", default="r")
    @click.option("--f_name", type=str, default=None)
    @click.option("--l_name", type=str, default=None)
    @click.option("--dob", type=click.DateTime(formats=["%d-%m-%Y"]), default=None)
    @click.option("--address", type=str, default=None)
    def runner(fun, f_name, l_name, dob, address):
        mapping[fun](f_name, l_name, dob, address)

    runner()
