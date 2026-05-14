import os
import sys
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STREAMLIT_APP_ROOT = os.path.join(PROJECT_ROOT, "streamlit_app")
for path in (PROJECT_ROOT, STREAMLIT_APP_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

import react_agent_service as service


class ReactAgentServiceTests(unittest.TestCase):
    def test_missing_credentials_returns_disabled_message(self):
        with patch("react_agent_service.get_secret", return_value=None):
            result = service.answer_with_react_agent("Show me the KPI dashboard")

        self.assertFalse(result["ok"])
        self.assertIn("ReAct Agent is disabled", result["answer"])

    def test_tool_input_validation_rejects_invalid_values(self):
        self.assertIn("error", service.get_revenue_by_city(""))
        self.assertIn("error", service.get_top_cities_by_revenue(26))
        self.assertIn("error", service.get_surge_patterns(24, 25))
        self.assertIn("error", service.predict_city_demand("Chicago", 25))

        too_many_cities = ",".join(f"City{i}" for i in range(11))
        self.assertIn("error", service.get_city_surge_comparison(too_many_cities))

    def test_city_tool_uses_query_parameters(self):
        malicious_city = "Chicago' OR 1=1 --"
        with patch("react_agent_service._run_query", return_value=[]) as query_mock:
            result = service.get_revenue_by_city(malicious_city)

        sql_text, parameters = query_mock.call_args.args
        self.assertEqual(parameters, [malicious_city])
        self.assertNotIn(malicious_city, sql_text)
        self.assertIn("No revenue data", result["message"])

    def test_comparison_tool_uses_parameterized_city_list(self):
        with patch("react_agent_service._run_query", return_value=[]) as query_mock:
            service.get_city_surge_comparison("New York,Los Angeles")

        sql_text, parameters = query_mock.call_args.args
        self.assertEqual(parameters, ["new york", "los angeles"])
        self.assertIn("?, ?", sql_text)
        self.assertNotIn("New York", sql_text)
        self.assertNotIn("Los Angeles", sql_text)

    def test_tool_outputs_do_not_include_passenger_or_driver_names(self):
        rows = [
            {
                "city": "Chicago",
                "total_rides": 4,
                "total_revenue": 100.0,
                "avg_fare": 25.0,
                "avg_surge": 1.2,
                "avg_rating": 4.8,
            }
        ]
        with patch("react_agent_service._run_query", return_value=rows):
            result = service.get_revenue_by_city("Chicago")

        serialized = str(result).lower()
        self.assertNotIn("passenger_name", serialized)
        self.assertNotIn("driver_name", serialized)
        self.assertNotIn("passenger", serialized)
        self.assertNotIn("driver", serialized)

    def test_silver_deep_dive_uses_parameters_and_sanitized_output(self):
        overview = [{"city": "Chicago", "total_rides": 5, "total_revenue": 150.0}]
        with patch("react_agent_service._run_query", side_effect=[overview, [], [], []]) as query_mock:
            result = service.get_silver_city_deep_dive("Chicago")

        first_sql, first_parameters = query_mock.call_args_list[0].args
        self.assertEqual(first_parameters, ["Chicago"])
        self.assertNotIn("Chicago", first_sql)
        serialized = str(result).lower()
        self.assertEqual(result["layer"], "silver")
        self.assertNotIn("passenger_name", serialized)
        self.assertNotIn("driver_name", serialized)

    def test_prediction_tool_uses_silver_layer_and_parameters(self):
        rows = [{"rides_last_6h": 12, "avg_rides_per_hour": 2.0, "avg_surge_last_6h": 1.4}]
        with patch("react_agent_service._run_query", return_value=rows) as query_mock:
            result = service.predict_city_demand("Dallas", 3)

        sql_text, parameters = query_mock.call_args.args
        self.assertEqual(parameters, ["Dallas"])
        self.assertIn("uber.silver.silver_obt", sql_text)
        self.assertEqual(result["layer"], "ml_predictions")
        self.assertEqual(result["predicted_rides"], 6.0)

    def test_external_market_context_uses_local_reference(self):
        result = service.get_external_market_context("San Diego")

        self.assertEqual(result["layer"], "external_context")
        self.assertEqual(result["city"], "San Diego")
        self.assertIn("market_archetype", result)

    def test_agent_tool_catalog_contains_specialized_layers(self):
        tool_names = {tool.__name__ for tool in service._get_tools()}

        self.assertIn("get_silver_city_deep_dive", tool_names)
        self.assertIn("predict_city_demand", tool_names)
        self.assertIn("get_external_market_context", tool_names)

    def test_agent_dependency_failure_is_stable(self):
        def fake_secret(key):
            return "configured"

        with patch("react_agent_service.get_secret", side_effect=fake_secret):
            with patch("react_agent_service._get_agent", side_effect=ImportError("langchain")):
                result = service.answer_with_react_agent("Show me the KPI dashboard")

        self.assertFalse(result["ok"])
        self.assertIn("dependencies are not installed", result["answer"])


if __name__ == "__main__":
    unittest.main()
