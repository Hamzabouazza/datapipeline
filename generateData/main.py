"""
Banking Data Pipeline - PySpark + DBT

Complete banking data pipeline that:
1. Generates realistic banking data (customers, accounts, transactions, cards, loans)
2. Loads it into PostgreSQL
3. Transforms it using DBT into a banking data warehouse
4. Generates analytics for fraud detection, risk assessment, and customer insights
"""

import sys
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


def run_banking_data_generator():
    """Run the banking data generator."""
    print("\n" + "=" * 60)
    print("🏦 STEP 1: Generating Banking Data with PySpark")
    print("=" * 60)

    from job.banking_generator import BankingDataGenerator
    from config import Config

    config = Config()
    generator = BankingDataGenerator(config)

    try:
        generator.run()
        return True
    except Exception as e:
        print(f"\n❌ Banking data generation failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        generator.stop()


def run_dbt_transformations():
    """Run DBT transformations to build the banking data warehouse."""
    print("\n" + "=" * 60)
    print("🔄 STEP 2: Running DBT Transformations")
    print("=" * 60)

    dbt_project_dir = Path(__file__).parent / "dbt_transform"

    if not dbt_project_dir.exists():
        print(f"❌ DBT project not found at: {dbt_project_dir}")
        print("   Please run: dbt init dbt_transform")
        return False

    try:
        # Test DBT connection
        print("\n🔍 Testing DBT connection...")
        result = subprocess.run(
            ["dbt", "debug", "--project-dir", str(dbt_project_dir)],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print("❌ DBT connection test failed")
            print(result.stderr)
            return False

        print("✅ DBT connection successful")

        # Run DBT models
        print("\n🏗️  Building DBT models...")
        result = subprocess.run(
            ["dbt", "run", "--project-dir", str(dbt_project_dir)],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print("✅ DBT models built successfully")
            print(result.stdout)

            # Run DBT tests
            print("\n🧪 Running DBT tests...")
            result = subprocess.run(
                ["dbt", "test", "--project-dir", str(dbt_project_dir)],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                print("✅ All DBT tests passed")
                print(result.stdout)
            else:
                print("⚠️  Some DBT tests failed")
                print(result.stdout)

            return True
        else:
            print("❌ DBT transformation failed")
            print(result.stderr)
            return False

    except FileNotFoundError:
        print("❌ DBT is not installed. Please run: pip install dbt-postgres")
        return False
    except Exception as e:
        print(f"❌ Error running DBT: {str(e)}")
        return False


def generate_dbt_docs():
    """Generate and serve DBT documentation."""
    print("\n" + "=" * 60)
    print("📚 Generating DBT Documentation")
    print("=" * 60)

    dbt_project_dir = Path(__file__).parent / "dbt_transform"

    try:
        # Generate docs
        print("\n📖 Generating documentation...")
        result = subprocess.run(
            ["dbt", "docs", "generate", "--project-dir", str(dbt_project_dir)],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print("✅ Documentation generated successfully")
            print("\n💡 To view documentation, run:")
            print(f"   cd {dbt_project_dir} && dbt docs serve")
            return True
        else:
            print("❌ Documentation generation failed")
            print(result.stderr)
            return False

    except Exception as e:
        print(f"❌ Error generating docs: {str(e)}")
        return False


def show_pipeline_summary():
    """Show summary of what was created."""
    print("\n" + "=" * 60)
    print("📊 BANKING DATA WAREHOUSE SUMMARY")
    print("=" * 60)

    print("\n🗄️  RAW TABLES (in PostgreSQL):")
    print("   ├── raw_branches          (Bank branches)")
    print("   ├── raw_customers         (Customer master data)")
    print("   ├── raw_accounts          (Customer accounts)")
    print("   ├── raw_cards             (Credit/Debit cards)")
    print("   ├── raw_loans             (Customer loans)")
    print("   └── raw_transactions      (All transactions)")

    print("\n🔄 DBT STAGING MODELS:")
    print("   ├── stg_customers         (Cleaned customer data)")
    print("   ├── stg_branches          (Branch data)")
    print("   ├── stg_accounts          (Account data with metrics)")
    print("   ├── stg_transactions      (Transaction data with enrichments)")
    print("   ├── stg_cards             (Card data)")
    print("   └── stg_loans             (Loan data)")

    print("\n⭐ DBT DIMENSION TABLES:")
    print("   ├── dim_customers         (Customer dimension with RFM)")
    print("   ├── dim_accounts          (Account dimension)")
    print("   ├── dim_branches          (Branch dimension)")
    print("   ├── dim_cards             (Card dimension)")
    print("   ├── dim_loans             (Loan dimension)")
    print("   └── dim_date              (Date dimension)")

    print("\n📈 DBT FACT TABLES:")
    print("   ├── fct_transactions           (All transactions - incremental)")
    print("   ├── fct_daily_account_balance  (Daily account balances)")
    print("   ├── fct_customer_daily_summary (Customer daily activity)")
    print("   ├── fct_fraud_events           (Fraud detection)")
    print("   ├── fct_loan_payments          (Loan payment tracking)")
    print("   └── fct_card_transactions      (Card-specific transactions)")

    print("\n📊 DBT ANALYTICS VIEWS:")
    print("   ├── fraud_detection_report      (Fraud patterns & risk)")
    print("   ├── customer_segmentation       (RFM & lifecycle segments)")
    print("   ├── transaction_trends          (Daily transaction trends)")
    print("   ├── account_health_dashboard    (Account monitoring)")
    print("   ├── loan_portfolio_analysis     (Loan performance)")
    print("   ├── branch_performance          (Branch metrics)")
    print("   ├── network_performance_analysis (5G network quality)")
    print("   └── credit_risk_assessment      (Credit risk scoring)")

    print("\n💡 SAMPLE QUERIES:")
    print("\n   -- Find high-risk fraud customers")
    print("   SELECT * FROM analytics.fraud_detection_report")
    print("   WHERE fraud_risk_level = 'High Risk';")
    print("\n   -- Customer segmentation")
    print("   SELECT customer_lifecycle_segment, COUNT(*)")
    print("   FROM analytics.customer_segmentation")
    print("   GROUP BY customer_lifecycle_segment;")
    print("\n   -- Credit risk analysis")
    print("   SELECT * FROM analytics.credit_risk_assessment")
    print("   WHERE overall_risk_level IN ('High Risk', 'Medium-High Risk');")

    print("\n🎯 NEXT STEPS:")
    print("   1. Query the analytics views for insights")
    print("   2. View DBT lineage: cd dbt_transform && dbt docs serve")
    print("   3. Connect Tableau/PowerBI to the warehouse")
    print("   4. Set up fraud alerts based on fct_fraud_events")
    print("   5. Schedule daily runs with Airflow/cron")
    print("=" * 60)


def main():
    """Main entry point for the banking pipeline."""
    parser = argparse.ArgumentParser(
        description="Banking Data Pipeline - PySpark + DBT"
    )
    parser.add_argument(
        "--job",
        type=str,
        default="full",
        choices=["full", "generate", "transform", "docs"],
        help="Pipeline step to run (default: full)",
    )
    parser.add_argument(
        "--skip-tests", action="store_true", help="Skip DBT tests during transformation"
    )

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🏦 BANKING DATA PIPELINE")
    print("   Realistic Banking Data Generation + DBT Transformation")
    print("=" * 60)

    start_time = datetime.now()
    success = True

    try:
        if args.job in ["full", "generate"]:
            success = run_banking_data_generator()
            if not success:
                sys.exit(1)

        if args.job in ["full", "transform"] and success:
            success = run_dbt_transformations()
            if not success:
                sys.exit(1)

        if args.job == "docs":
            success = generate_dbt_docs()
            if not success:
                sys.exit(1)

        if success:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print("\n" + "=" * 60)
            print(f"✅ PIPELINE COMPLETED SUCCESSFULLY")
            print(f"   Total time: {duration:.2f} seconds")
            print("=" * 60)

            if args.job == "full":
                show_pipeline_summary()

    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ PIPELINE FAILED: {str(e)}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
