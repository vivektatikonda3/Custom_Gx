import pandas as pd

import sys
import json
import platform
import os,requests
from pathlib import Path
import glob

from src.DataValidationContext import GEDataValidationContext
from src.BatchRequest import getBatchRequest 
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from custom_Expectations.cust_try import ExpectColumnValuesToBeWithinFreshnessExpectation
from great_expectations.expectations.registry import get_expectation_impl

from configs_conn.config import snowflake_connect as snowflake_conn


# Register the custom expectation if not already done
try:
    get_expectation_impl("expect_column_values_to_be_within_freshness_expectation")
except Exception as e:
    print(f"Custom expectation registration issue: {str(e)}")



def createExpectationSuite(context,suitename):
    context.create_expectation_suite(expectation_suite_name=suitename, overwrite_existing=True)
    
    
def createExpectations(context,suitename,local_batch_request,pandasdataframe):
    # Creating the validator which takes the batch request and expectation suite name
    validator = context.get_validator(
        batch_request=local_batch_request, expectation_suite_name=suitename
    )
    
    # Creating new validations based on data knowledge. 
    # Add additional expectations as per the business needs 
    # validator.expect_column_values_to_be_in_set("GENDER",["female","male"])
    # validator.expect_column_min_to_be_between("math score",1,100)

    validator.expect_column_values_to_be_within_freshness_expectation(column="CREATED_DATE", freshness_days=30)
    # validator.expect_column_mean_to_be_between("VENDOR_ID",2,14)
    
    #Saving the expectation 
    validator.save_expectation_suite(discard_failed_expectations=False)