"""
Data Validation Service
======================

Generic service for data quality validation using Great Expectations.
Provides flexible validation without assuming specific data structures.
"""

import great_expectations as gx
from pyspark.sql import DataFrame
import tempfile
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ValidationService:
    """
    Generic data validation service using Great Expectations
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.gx_context = self._setup_great_expectations()
        
    def _setup_great_expectations(self):
        """Setup Great Expectations context with Spark"""
        try:
            # Create temporary directory for GX context
            temp_dir = tempfile.mkdtemp()
            gx_dir = os.path.join(temp_dir, "gx")
            
            # Initialize Great Expectations context
            context = gx.get_context(context_root_dir=gx_dir)
            
            # Add Spark datasource
            datasource = context.data_sources.add_spark(
                name="spark_datasource",
                spark_session=self.spark
            )
            
            print("✅ Great Expectations setup completed successfully")
            return context
            
        except Exception as e:
            print(f"❌ Failed to setup Great Expectations: {e}")
            raise
    
    def create_basic_suite(self, suite_name):
        """
        Create a basic expectation suite with common data quality checks
        
        Args:
            suite_name: Name for the expectation suite
        
        Returns:
            str: Suite name
        """
        try:
            # Create new expectation suite
            suite = self.gx_context.suites.add(gx.ExpectationSuite(name=suite_name))
            
            # Add only basic expectations that work for any dataset
            basic_expectations = [
                # Table should have data
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
            ]
            
            # Add basic expectations to suite
            for expectation in basic_expectations:
                suite.add_expectation(expectation)
            
            print(f"✅ Created basic expectation suite: {suite_name}")
            return suite_name
            
        except Exception as e:
            print(f"❌ Failed to create suite {suite_name}: {e}")
            raise
    
    def add_column_expectations(self, df, suite_name):
        """
        Add expectations based on DataFrame schema (generic approach)
        
        Args:
            df: DataFrame to analyze
            suite_name: Name of expectation suite
        """
        try:
            suite = self.gx_context.suites.get(suite_name)
            schema = df.schema
            
            print(f"📊 Adding expectations for {len(schema.fields)} columns...")
            
            # Add column existence for all columns
            for field in schema.fields:
                col_name = field.name
                
                # Every column should exist
                suite.add_expectation(
                    gx.expectations.ExpectColumnToExist(column=col_name)
                )
                
                # Heuristic: columns with 'id' in name should be unique and not null
                if 'id' in col_name.lower():
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToNotBeNull(column=col_name)
                    )
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeUnique(column=col_name)
                    )
                
                # Heuristic: columns with 'email' should match email pattern
                if 'email' in col_name.lower():
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToMatchRegex(
                            column=col_name,
                            regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                        )
                    )
                
                # Heuristic: columns with 'name' should not be null
                if 'name' in col_name.lower():
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToNotBeNull(column=col_name)
                    )
            
            # Update suite
            self.gx_context.suites.add(suite)
            print(f"✅ Added column-based expectations to {suite_name}")
            
        except Exception as e:
            print(f"❌ Failed to add column expectations: {e}")
    
    def validate_dataframe(self, df, suite_name, batch_id=None):
        """
        Validate DataFrame against expectation suite
        
        Args:
            df: DataFrame to validate
            suite_name: Name of expectation suite
            batch_id: Optional batch identifier
        
        Returns:
            Dict: Validation results
        """
        if batch_id is None:
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Get datasource and create asset
            datasource = self.gx_context.data_sources.get("spark_datasource")
            asset_name = f"data_{batch_id}"
            data_asset = datasource.add_dataframe_asset(name=asset_name)
            
            # Create batch request
            batch_request = data_asset.build_batch_request(dataframe=df)
            
            # Get expectation suite
            suite = self.gx_context.suites.get(suite_name)
            
            # Create validator and run validation
            validator = self.gx_context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )
            
            # Run validation
            results = validator.validate()
            
            # Process results
            validation_summary = self._process_validation_results(results)
            
            return validation_summary
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "success_percent": 0,
                "total_expectations": 0,
                "successful_expectations": 0,
                "failed_expectations": [],
                "batch_id": batch_id
            }
    
    def _process_validation_results(self, results):
        """Process Great Expectations validation results"""
        
        total_expectations = len(results.results)
        successful_expectations = len([r for r in results.results if r.success])
        failed_expectations = []
        
        # Collect failed expectations with details
        for result in results.results:
            if not result.success:
                failed_expectations.append({
                    "expectation_type": result.expectation_config.expectation_type,
                    "column": result.expectation_config.kwargs.get("column", "N/A"),
                    "details": str(result.result)
                })
        
        success_percent = (successful_expectations / total_expectations * 100) if total_expectations > 0 else 0
        
        return {
            "success": results.success,
            "success_percent": success_percent,
            "total_expectations": total_expectations,
            "successful_expectations": successful_expectations,
            "failed_expectations": failed_expectations,
            "batch_id": results.meta.get("batch_id", "unknown"),
            "run_time": results.meta.get("run_time", datetime.now().isoformat())
        }
    
    def validate_data(self, df, dataset_name="data"):
        """
        Complete validation workflow for any dataset
        
        Args:
            df: DataFrame to validate
            dataset_name: Name for the dataset (used in suite name)
        
        Returns:
            Dict: Validation results
        """
        try:
            print(f"🔍 Validating {dataset_name}...")
            
            # Create suite name
            suite_name = f"{dataset_name}_validation_suite"
            
            # Create basic suite
            self.create_basic_suite(suite_name)
            
            # Add column-based expectations
            self.add_column_expectations(df, suite_name)
            
            # Run validation
            results = self.validate_dataframe(df, suite_name)
            
            # Print summary
            self._print_validation_summary(results, dataset_name)
            
            return results
            
        except Exception as e:
            print(f"❌ Data validation failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _print_validation_summary(self, results, dataset_name):
        """Print formatted validation summary"""
        
        print(f"\n📋 VALIDATION SUMMARY: {dataset_name}")
        print("=" * 50)
        
        if results.get("success", False):
            print(f"✅ Validation PASSED")
        else:
            print(f"❌ Validation FAILED")
        
        print(f"   Success rate: {results.get('success_percent', 0):.1f}%")
        print(f"   Expectations: {results.get('successful_expectations', 0)}/{results.get('total_expectations', 0)} passed")
        
        # Show failed expectations
        failed = results.get("failed_expectations", [])
        if failed:
            print(f"\n   Failed expectations:")
            for i, fail in enumerate(failed[:5]):  # Show first 5 failures
                column = fail.get('column', 'N/A')
                exp_type = fail.get('expectation_type', 'Unknown').replace('expect_', '').replace('_', ' ')
                print(f"     {i+1}. {exp_type} on column '{column}'")
            
            if len(failed) > 5:
                print(f"     ... and {len(failed) - 5} more failures")
        
        print("=" * 50)
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            print("✅ ValidationService cleanup completed")
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")