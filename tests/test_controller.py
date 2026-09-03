import unittest
from unittest.mock import Mock, patch

from dsimaging_admin import controller


class ControllerClientTests(unittest.TestCase):
    @staticmethod
    def response(payload):
        response = Mock()
        response.status_code = 200
        response.json.return_value = payload
        return response

    @patch("dsimaging_admin.controller.requests.get")
    def test_datasets_sends_operator_bearer_token(self, request):
        request.return_value = self.response({"datasets": []})

        self.assertEqual(
            controller.datasets("http://controller:8080", token=" secret "),
            [],
        )

        request.assert_called_once_with(
            "http://controller:8080/datasets",
            timeout=5.0,
            headers={"Authorization": "Bearer secret"},
        )

    @patch("dsimaging_admin.controller.requests.post")
    def test_reconcile_sends_token_without_exposing_it_in_url(self, request):
        request.return_value = self.response({"status": "ok"})

        payload = controller.reconcile(
            "http://controller:8080", "study", token="secret")

        self.assertEqual(payload, {"status": "ok"})
        request.assert_called_once_with(
            "http://controller:8080/reconcile/study",
            timeout=30.0,
            headers={"Authorization": "Bearer secret"},
        )

    @patch("dsimaging_admin.controller.requests.get")
    def test_health_is_called_without_operator_token(self, request):
        request.return_value = self.response({"status": "ok"})

        self.assertEqual(
            controller.health("http://controller:8080"),
            {"status": "ok"},
        )

        request.assert_called_once_with(
            "http://controller:8080/health", timeout=5.0, headers={})


if __name__ == "__main__":
    unittest.main()
