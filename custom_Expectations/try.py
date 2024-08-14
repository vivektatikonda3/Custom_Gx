import custom_Expectations.cust_try as cust_try
from great_expectations.expectations.registry import _registered_expectations

# List all registered expectations
registered_expectations = list(_registered_expectations.keys())
print(f"Registered Expectations: {registered_expectations}")

# Check if the custom expectation is registered
if "expect_column_values_to_be_within_freshness_expectation" in registered_expectations:
    print("Custom expectation is registered correctly.")
else:
    print("Custom expectation is NOT registered.")
