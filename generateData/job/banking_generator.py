"""
Banking Data Generator

Generates realistic banking data including customers, accounts, transactions,
cards, loans, and branches with fraud detection patterns.
"""

from pyspark.sql.types import (
    LongType,
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.functions import col, expr, when, lit
import dbldatagen as dg
from core.DataCore import DataCore
from config import Config
from datetime import datetime
import random


class BankingDataGenerator:
    """Generate realistic banking data."""

    def __init__(self, config: Config):
        self.config = config
        self.data_core = DataCore(
            app_name="BankingDataGenLocal", master=config.spark_master
        )

        if config.jar_path:
            self.data_core.with_jdbc_jar(config.jar_path)

        self.data_core.with_shuffle_partitions(config.shuffle_partitions)
        self.spark = self.data_core.spark

        # Configuration
        self.num_customers = 5000
        self.num_accounts = 8000  # Some customers have multiple accounts
        self.num_transactions = 50000
        self.num_branches = 50
        self.num_cards = 6000
        self.num_loans = 2000
        self.partitions = 8

    def generate_branches(self):
        """Generate bank branch data."""
        print("\n🏦 Generating branch data...")

        cities = [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
            "San Diego",
            "Dallas",
            "San Jose",
            "Austin",
            "Jacksonville",
            "Fort Worth",
            "Columbus",
            "Charlotte",
            "San Francisco",
            "Indianapolis",
            "Seattle",
            "Denver",
            "Boston",
        ]

        branch_types = ["Main Branch", "Regional Branch", "ATM Only", "Mobile Branch"]

        branch_spec = (
            dg.DataGenerator(
                self.spark,
                name="branches",
                rows=self.num_branches,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn("branch_code", StringType(), format="BR%05d", baseColumn="id")
            .withColumn(
                "branch_name", StringType(), template=r"\\w \\w Branch", random=True
            )
            .withColumn("city", StringType(), values=cities, random=True)
            .withColumn(
                "state",
                StringType(),
                values=["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"],
                random=True,
            )
            .withColumn(
                "branch_type",
                StringType(),
                values=branch_types,
                weights=[40, 35, 15, 10],
                random=True,
            )
            .withColumn(
                "latitude", DoubleType(), minValue=25.0, maxValue=48.0, random=True
            )
            .withColumn(
                "longitude", DoubleType(), minValue=-125.0, maxValue=-65.0, random=True
            )
        )

        df = branch_spec.build()
        df = df.withColumn("latitude", expr("round(latitude, 6)"))
        df = df.withColumn("longitude", expr("round(longitude, 6)"))

        self._write_to_postgres(df, "raw_branches")
        return df

    def generate_customers(self):
        """Generate customer dimension data."""
        print("\n👥 Generating customer data...")

        customer_segments = ["Retail", "Premium", "Business", "VIP"]
        risk_profiles = ["Low", "Medium", "High"]
        employment_status = [
            "Employed",
            "Self-Employed",
            "Retired",
            "Student",
            "Unemployed",
        ]

        customer_spec = (
            dg.DataGenerator(
                self.spark,
                name="customers",
                rows=self.num_customers,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn("customer_id", StringType(), format="CUST%08d", baseColumn="id")
            .withColumn("first_name", StringType(), template=r"\\w", random=True)
            .withColumn("last_name", StringType(), template=r"\\w", random=True)
            .withColumn("email", StringType(), template=r"\\w.\\w@\\w.com", random=True)
            .withColumn(
                "phone",
                StringType(),
                template=r"+1-\d\d\d-\d\d\d-\d\d\d\d",
                random=True,
            )
            .withColumn(
                "date_of_birth",
                "timestamp",
                begin="1950-01-01 00:00:00",
                end="2005-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "registration_date",
                "timestamp",
                begin="2015-01-01 00:00:00",
                end="2024-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "customer_segment",
                StringType(),
                values=customer_segments,
                weights=[60, 25, 10, 5],
                random=True,
            )
            .withColumn(
                "risk_profile",
                StringType(),
                values=risk_profiles,
                weights=[70, 25, 5],
                random=True,
            )
            .withColumn(
                "credit_score", IntegerType(), minValue=300, maxValue=850, random=True
            )
            .withColumn(
                "annual_income",
                DoubleType(),
                minValue=15000,
                maxValue=500000,
                random=True,
            )
            .withColumn(
                "employment_status",
                StringType(),
                values=employment_status,
                weights=[60, 15, 10, 10, 5],
                random=True,
            )
            .withColumn(
                "branch_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_branches - 1,
                random=True,
            )
            .withColumn(
                "is_active",
                BooleanType(),
                values=[True, False],
                weights=[95, 5],
                random=True,
            )
        )

        df = customer_spec.build()
        df = df.withColumn("annual_income", expr("round(annual_income, 2)"))

        self._write_to_postgres(df, "raw_customers")
        return df

    def generate_accounts(self):
        """Generate account data."""
        print("\n💰 Generating account data...")

        account_types = ["Checking", "Savings", "Money Market", "Credit Card", "Loan"]
        account_status = ["Active", "Dormant", "Closed", "Frozen"]
        currencies = ["USD", "EUR", "GBP", "CAD"]

        account_spec = (
            dg.DataGenerator(
                self.spark,
                name="accounts",
                rows=self.num_accounts,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn("account_id", StringType(), format="ACC%010d", baseColumn="id")
            .withColumn(
                "customer_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_customers - 1,
                random=True,
            )
            .withColumn(
                "account_type",
                StringType(),
                values=account_types,
                weights=[35, 30, 10, 15, 10],
                random=True,
            )
            .withColumn(
                "account_status",
                StringType(),
                values=account_status,
                weights=[85, 8, 5, 2],
                random=True,
            )
            .withColumn(
                "currency",
                StringType(),
                values=currencies,
                weights=[85, 8, 4, 3],
                random=True,
            )
            .withColumn(
                "balance", DoubleType(), minValue=-5000, maxValue=250000, random=True
            )
            .withColumn(
                "overdraft_limit", DoubleType(), minValue=0, maxValue=5000, random=True
            )
            .withColumn(
                "interest_rate", DoubleType(), minValue=0.01, maxValue=5.5, random=True
            )
            .withColumn(
                "opening_date",
                "timestamp",
                begin="2015-01-01 00:00:00",
                end="2024-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "branch_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_branches - 1,
                random=True,
            )
        )

        df = account_spec.build()

        # Round numeric columns
        df = df.withColumn("balance", expr("round(balance, 2)"))
        df = df.withColumn("overdraft_limit", expr("round(overdraft_limit, 2)"))
        df = df.withColumn("interest_rate", expr("round(interest_rate, 2)"))

        # Set overdraft only for checking accounts
        df = df.withColumn(
            "overdraft_limit",
            when(col("account_type") == "Checking", col("overdraft_limit")).otherwise(
                0.0
            ),
        )

        self._write_to_postgres(df, "raw_accounts")
        return df

    def generate_transactions(self):
        """Generate transaction data matching your schema."""
        print("\n💳 Generating transaction data...")

        transaction_types = [
            "Transfer",
            "Deposit",
            "Withdrawal",
            "Payment",
            "ATM Withdrawal",
            "Online Purchase",
            "Bill Payment",
            "Mobile Payment",
            "Wire Transfer",
            "Direct Debit",
        ]

        transaction_status = ["Completed", "Pending", "Failed", "Cancelled"]

        devices = [
            "Mobile App - iOS",
            "Mobile App - Android",
            "Web Browser - Chrome",
            "Web Browser - Safari",
            "ATM",
            "POS Terminal",
            "Branch Terminal",
        ]

        network_slices = [
            "Slice-001",
            "Slice-002",
            "Slice-003",
            "Slice-004",
            "Slice-005",
        ]

        transaction_spec = (
            dg.DataGenerator(
                self.spark,
                name="transactions",
                rows=self.num_transactions,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn(
                "transaction_id", StringType(), format="TXN%012d", baseColumn="id"
            )
            .withColumn(
                "sender_account_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_accounts - 1,
                random=True,
            )
            .withColumn(
                "receiver_account_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_accounts - 1,
                random=True,
            )
            .withColumn(
                "transaction_amount",
                DoubleType(),
                minValue=0.01,
                maxValue=50000,
                random=True,
            )
            .withColumn(
                "transaction_type",
                StringType(),
                values=transaction_types,
                weights=[20, 15, 10, 12, 8, 10, 8, 7, 5, 5],
                random=True,
            )
            .withColumn(
                "timestamp",
                "timestamp",
                begin="2024-01-01 00:00:00",
                end="2024-12-31 23:59:59",
                interval="1 minute",
                random=True,
            )
            .withColumn(
                "transaction_status",
                StringType(),
                values=transaction_status,
                weights=[88, 8, 3, 1],
                random=True,
            )
            .withColumn(
                "fraud_flag",
                BooleanType(),
                values=[False, True],
                weights=[97, 3],
                random=True,
            )
            .withColumn(
                "latitude", DoubleType(), minValue=25.0, maxValue=48.0, random=True
            )
            .withColumn(
                "longitude", DoubleType(), minValue=-125.0, maxValue=-65.0, random=True
            )
            .withColumn(
                "device_used",
                StringType(),
                values=devices,
                weights=[25, 25, 15, 10, 10, 10, 5],
                random=True,
            )
            .withColumn(
                "network_slice_id", StringType(), values=network_slices, random=True
            )
            .withColumn(
                "latency_ms", IntegerType(), minValue=10, maxValue=500, random=True
            )
            .withColumn(
                "slice_bandwidth_mbps",
                IntegerType(),
                minValue=10,
                maxValue=1000,
                random=True,
            )
            .withColumn(
                "pin_code", IntegerType(), minValue=1000, maxValue=9999, random=True
            )
        )

        df = transaction_spec.build()

        # Round and format columns
        df = df.withColumn("transaction_amount", expr("round(transaction_amount, 2)"))
        df = df.withColumn("latitude", expr("round(latitude, 6)"))
        df = df.withColumn("longitude", expr("round(longitude, 6)"))
        df = df.withColumn("geolocation", expr("concat(latitude, ',', longitude)"))

        # Make receiver null for deposits and withdrawals
        df = df.withColumn(
            "receiver_account_id",
            when(
                col("transaction_type").isin(
                    ["Deposit", "Withdrawal", "ATM Withdrawal"]
                ),
                lit(None),
            ).otherwise(col("receiver_account_id")),
        )

        # Increase fraud probability for high amounts and certain types
        df = df.withColumn(
            "fraud_flag",
            when(
                (col("transaction_amount") > 10000)
                & (col("transaction_type").isin(["Wire Transfer", "Online Purchase"])),
                expr("CASE WHEN rand() < 0.15 THEN true ELSE fraud_flag END"),
            ).otherwise(col("fraud_flag")),
        )

        # Set failed status for fraud transactions
        df = df.withColumn(
            "transaction_status",
            when(
                col("fraud_flag") == True,
                expr(
                    "CASE WHEN rand() < 0.7 THEN 'Failed' ELSE transaction_status END"
                ),
            ).otherwise(col("transaction_status")),
        )

        # Drop intermediate columns
        df = df.drop("latitude", "longitude")

        self._write_to_postgres(df, "raw_transactions")
        return df

    def generate_cards(self):
        """Generate credit/debit card data."""
        print("\n💳 Generating card data...")

        card_types = ["Debit", "Credit", "Prepaid"]
        card_networks = ["Visa", "Mastercard", "American Express", "Discover"]
        card_status = ["Active", "Blocked", "Expired", "Lost"]

        card_spec = (
            dg.DataGenerator(
                self.spark,
                name="cards",
                rows=self.num_cards,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn(
                "card_number",
                StringType(),
                template=r"\d\d\d\d-\d\d\d\d-\d\d\d\d-\d\d\d\d",
                random=True,
            )
            .withColumn(
                "account_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_accounts - 1,
                random=True,
            )
            .withColumn(
                "customer_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_customers - 1,
                random=True,
            )
            .withColumn(
                "card_type",
                StringType(),
                values=card_types,
                weights=[50, 35, 15],
                random=True,
            )
            .withColumn(
                "card_network",
                StringType(),
                values=card_networks,
                weights=[40, 35, 15, 10],
                random=True,
            )
            .withColumn(
                "card_status",
                StringType(),
                values=card_status,
                weights=[85, 8, 5, 2],
                random=True,
            )
            .withColumn(
                "issue_date",
                "timestamp",
                begin="2020-01-01 00:00:00",
                end="2024-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "expiry_date",
                "timestamp",
                begin="2025-01-01 00:00:00",
                end="2029-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "credit_limit", DoubleType(), minValue=500, maxValue=50000, random=True
            )
            .withColumn(
                "available_credit",
                DoubleType(),
                minValue=0,
                maxValue=50000,
                random=True,
            )
        )

        df = card_spec.build()

        # Round credit amounts
        df = df.withColumn("credit_limit", expr("round(credit_limit, 2)"))
        df = df.withColumn("available_credit", expr("round(available_credit, 2)"))

        # Set credit limit only for credit cards
        df = df.withColumn(
            "credit_limit",
            when(col("card_type") == "Credit", col("credit_limit")).otherwise(0.0),
        )

        df = df.withColumn(
            "available_credit",
            when(
                col("card_type") == "Credit",
                expr(
                    "CASE WHEN available_credit > credit_limit THEN credit_limit ELSE available_credit END"
                ),
            ).otherwise(0.0),
        )

        self._write_to_postgres(df, "raw_cards")
        return df

    def generate_loans(self):
        """Generate loan data."""
        print("\n🏠 Generating loan data...")

        loan_types = ["Personal", "Mortgage", "Auto", "Student", "Business"]
        loan_status = ["Active", "Paid Off", "Defaulted", "In Arrears"]

        loan_spec = (
            dg.DataGenerator(
                self.spark,
                name="loans",
                rows=self.num_loans,
                partitions=self.partitions,
                randomSeedMethod="hash_fieldname",
            )
            .withIdOutput()
            .withColumn("loan_id", StringType(), format="LOAN%08d", baseColumn="id")
            .withColumn(
                "customer_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_customers - 1,
                random=True,
            )
            .withColumn(
                "account_id",
                IntegerType(),
                minValue=0,
                maxValue=self.num_accounts - 1,
                random=True,
            )
            .withColumn(
                "loan_type",
                StringType(),
                values=loan_types,
                weights=[30, 25, 25, 12, 8],
                random=True,
            )
            .withColumn(
                "loan_amount", DoubleType(), minValue=5000, maxValue=500000, random=True
            )
            .withColumn(
                "outstanding_balance",
                DoubleType(),
                minValue=0,
                maxValue=500000,
                random=True,
            )
            .withColumn(
                "interest_rate", DoubleType(), minValue=2.5, maxValue=18.5, random=True
            )
            .withColumn(
                "term_months",
                IntegerType(),
                values=[12, 24, 36, 60, 120, 180, 240, 360],
                weights=[5, 10, 15, 20, 15, 15, 10, 10],
                random=True,
            )
            .withColumn(
                "monthly_payment",
                DoubleType(),
                minValue=100,
                maxValue=5000,
                random=True,
            )
            .withColumn(
                "start_date",
                "timestamp",
                begin="2020-01-01 00:00:00",
                end="2024-12-31 23:59:00",
                interval="1 day",
                random=True,
            )
            .withColumn(
                "loan_status",
                StringType(),
                values=loan_status,
                weights=[70, 20, 5, 5],
                random=True,
            )
        )

        df = loan_spec.build()

        # Round amounts
        df = df.withColumn("loan_amount", expr("round(loan_amount, 2)"))
        df = df.withColumn("outstanding_balance", expr("round(outstanding_balance, 2)"))
        df = df.withColumn("interest_rate", expr("round(interest_rate, 2)"))
        df = df.withColumn("monthly_payment", expr("round(monthly_payment, 2)"))

        # Ensure outstanding balance <= loan amount
        df = df.withColumn(
            "outstanding_balance",
            expr(
                "CASE WHEN outstanding_balance > loan_amount THEN loan_amount * 0.6 ELSE outstanding_balance END"
            ),
        )

        self._write_to_postgres(df, "raw_loans")
        return df

    def _write_to_postgres(self, df, table_name):
        """Write DataFrame to PostgreSQL."""
        print(f"   ✍️  Writing to table: {table_name}")

        df.write.format("jdbc").option("url", self.config.jdbc_url).option(
            "dbtable", table_name
        ).option("user", self.config.postgres_user).option(
            "password", self.config.postgres_password
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()

        count = df.count()
        print(f"   ✅ Written {count} rows to {table_name}")

        # Show sample
        print(f"\n   Sample data from {table_name}:")
        df.show(5, truncate=False)

    def run(self):
        """Run the complete banking data generation pipeline."""
        print("\n" + "=" * 60)
        print("🏦 Starting Banking Data Generation Pipeline")
        print("=" * 60)

        start_time = datetime.now()

        # Generate all tables in proper order
        self.generate_branches()
        self.generate_customers()
        self.generate_accounts()
        self.generate_cards()
        self.generate_loans()
        self.generate_transactions()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 60)
        print(f"✅ Banking data generation completed in {duration:.2f} seconds")
        print("=" * 60)
        print("\n📊 Generated tables:")
        print(f"  - raw_branches       ({self.num_branches} rows)")
        print(f"  - raw_customers      ({self.num_customers} rows)")
        print(f"  - raw_accounts       ({self.num_accounts} rows)")
        print(f"  - raw_cards          ({self.num_cards} rows)")
        print(f"  - raw_loans          ({self.num_loans} rows)")
        print(f"  - raw_transactions   ({self.num_transactions} rows)")
        print("\n🎯 Ready for DBT transformation!")
        print("=" * 60)

    def stop(self):
        """Stop Spark session."""
        self.data_core.stop()


if __name__ == "__main__":
    config = Config()
    generator = BankingDataGenerator(config)

    try:
        generator.run()
    finally:
        generator.stop()
