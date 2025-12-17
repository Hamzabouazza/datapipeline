"""
Main entry point for data generation pipeline.

This script orchestrates the execution of data generation jobs.
"""

import sys
import argparse


def run_device_data_generator():
    """Run the device data generator job."""
    print("Starting Device Data Generator...")
    from job import dbg

    print("Device Data Generator completed!")


def main():
    """Main entry point with command-line arguments."""
    parser = argparse.ArgumentParser(description="Data Generation Pipeline")
    parser.add_argument(
        "--job",
        type=str,
        default="device",
        choices=["device"],
        help="Job to run (default: device)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print(f"Data Pipeline - Running job: {args.job}")
    print("=" * 60)

    try:
        if args.job == "device":
            run_device_data_generator()

        print("\n" + "=" * 60)
        print("✓ Pipeline completed successfully!")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Pipeline failed: {str(e)}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
