import pandas as pd
import numpy as np


def return_cost_per_run(test_data: pd.DataFrame):
    test_data["part_of_costs_per_run"] = (
        test_data["duration"] / test_data["sum_duration_serviceName"]
    )

    test_data["count_service_ocurrences_by_date"] = test_data.groupby(
        by=["date", "serviceName"]
    )["serviceName"].transform("count")
    test_data["part_of_costs_per_run"] = np.where(
        (test_data["sum_duration_serviceName"] == 0)
        & (test_data["total_charge_of_serviceName"] != 0)
        & (test_data["total_charge_of_serviceName"].isnull() == False),
        test_data["count_service_ocurrences_by_date"],
        test_data["part_of_costs_per_run"],
    )

    test_data["cost_per_run"] = (
        test_data["total_charge_of_serviceName"] * test_data["part_of_costs_per_run"]
    )
    return test_data
