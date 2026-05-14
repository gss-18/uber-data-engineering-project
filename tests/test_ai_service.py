import os
import sys
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STREAMLIT_APP_ROOT = os.path.join(PROJECT_ROOT, "streamlit_app")
for path in (PROJECT_ROOT, STREAMLIT_APP_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

from ai_service import build_chat_messages, build_dashboard_context, build_insights_prompt, SYSTEM_PROMPT


class AiServiceTests(unittest.TestCase):
    def test_live_feed_context_excludes_passenger_and_driver_names(self):
        data = {
            "kpis": {"total_rides": 10, "total_revenue": 250.75},
            "live_feed": [
                {
                    "passenger_name": "Jane Passenger",
                    "driver_name": "Dan Driver",
                    "pickup_city": "Chicago",
                    "vehicle_type": "UberX",
                    "total_fare": 31.25,
                    "distance_miles": 8.4,
                    "duration_minutes": 18,
                    "surge_multiplier": 1.2,
                    "rating": 4.8,
                }
            ],
        }

        context = build_dashboard_context(data)
        recent_ride = context["recent_rides"][0]

        self.assertNotIn("passenger_name", recent_ride)
        self.assertNotIn("driver_name", recent_ride)
        self.assertNotIn("Jane Passenger", str(context))
        self.assertNotIn("Dan Driver", str(context))
        self.assertEqual(recent_ride["pickup_city"], "Chicago")

    def test_prompt_uses_context_and_rejects_sql_behavior(self):
        context = build_dashboard_context({"kpis": {}, "live_feed": []})
        prompt = build_insights_prompt(context)

        self.assertIn("Dashboard context", prompt)
        self.assertIn("complete executive insight brief", prompt)
        self.assertIn("Recommended actions", prompt)
        self.assertIn("do not write SQL", SYSTEM_PROMPT)
        self.assertIn("ride_id", SYSTEM_PROMPT)
        self.assertIn("total_fare", SYSTEM_PROMPT)

    def test_insights_context_keeps_broader_dashboard_slices(self):
        data = {
            "by_city": [{"city": f"City {idx}"} for idx in range(30)],
            "live_feed": [{"pickup_city": f"City {idx}"} for idx in range(30)],
        }

        context = build_dashboard_context(data)

        self.assertEqual(len(context["rides_by_city"]), 25)
        self.assertEqual(len(context["recent_rides"]), 25)

    def test_missing_api_key_message_mentions_gemini(self):
        from ai_service import generate_insights

        old_key = os.environ.pop("GEMINI_API_KEY", None)
        try:
            ok, message = generate_insights({"kpis": {}, "live_feed": []})
        finally:
            if old_key is not None:
                os.environ["GEMINI_API_KEY"] = old_key

        self.assertFalse(ok)
        self.assertIn("Gemini is disabled", message)

    def test_chat_messages_include_bounded_history_and_latest_question(self):
        context = build_dashboard_context({"kpis": {"total_rides": 42}})
        history = [
            {"role": "user", "content": f"question {idx}"}
            for idx in range(10)
        ]

        messages = build_chat_messages(context, history, "What changed in demand?")

        self.assertIn("dashboard JSON context", messages[0]["content"])
        self.assertNotIn("question 0", str(messages))
        self.assertIn("question 9", str(messages))
        self.assertEqual(messages[-1], {"role": "user", "content": "What changed in demand?"})


if __name__ == "__main__":
    unittest.main()
