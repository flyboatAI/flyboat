import multiprocessing

import pytest
from fastapi.testclient import TestClient

from app import app
from config import setting
from core.engine_execute_pool import ExecutePool

client = TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def setup_teardown():
    # load_dotenv('.env.test')
    pool = multiprocessing.Pool(processes=4)
    ExecutePool.set_pool(pool)
    ExecutePool.set_manager_dict(multiprocessing.Manager().dict())
    yield
    pool.terminate()


@pytest.mark.parametrize(
    "version_id, expected_response",
    [
        (
            "63962cfd-40fa-44c9-80da-981b4481fecd",
            {
                "code": 0,
                "data": {
                    "MSE": [{"evaluate": "r2", "value": 0.39640401254263}],
                    "cost": [
                        {"cost": 159636556.4971246},
                        {"cost": 352348253.4021632},
                        {"cost": 545070398.7835033},
                        {"cost": 737782095.6885421},
                        {"cost": 930493792.5935806},
                        {"cost": 1123205489.4986193},
                        {"cost": 1315927634.8799598},
                        {"cost": 1508639331.7849982},
                        {"cost": 1701351028.6900368},
                        {"cost": 1894062725.5950754},
                        {"cost": 2086774422.500114},
                        {"cost": 2279496567.881454},
                        {"cost": 2472208264.786493},
                        {"cost": 2664919961.6915317},
                        {"cost": 2857631658.59657},
                        {"cost": 3050343355.5016093},
                        {"cost": 3243065500.882949},
                        {"cost": 3435777197.7879877},
                        {"cost": 3628488894.693026},
                        {"cost": 3821200591.598065},
                    ],
                },
                "message": "",
            },
        ),
        (
            "6511a1d7-d9f2-48ad-850b-e9a3e29bd351",
            {
                "code": 0,
                "data": {
                    "cost": [
                        {"ci": 0.05, "value": 195.19425776243776},
                        {"ci": 0.1, "value": 196.73031147585445},
                        {"ci": 0.15, "value": 198.27862614414533},
                        {"ci": 0.2, "value": 199.84576732720055},
                        {"ci": 0.25, "value": 201.43885932094477},
                        {"ci": 0.3, "value": 203.06583821688457},
                        {"ci": 0.35, "value": 204.73577521676975},
                        {"ci": 0.4, "value": 206.4593079089907},
                        {"ci": 0.45, "value": 208.24923846406986},
                        {"ci": 0.5, "value": 210.12139526138222},
                        {"ci": 0.55, "value": 212.09592345068032},
                        {"ci": 0.6, "value": 214.19930318938736},
                        {"ci": 0.65, "value": 216.46766801852016},
                        {"ci": 0.7, "value": 218.95260292113403},
                        {"ci": 0.75, "value": 221.73208475758585},
                        {"ci": 0.8, "value": 224.93333936420157},
                        {"ci": 0.85, "value": 228.7879542796844},
                        {"ci": 0.9, "value": 233.79769170846532},
                        {"ci": 0.95, "value": 241.48619539098058},
                    ]
                },
                "message": "",
            },
        ),
        (
            "2goLd5uA5VcezqIcWFw4z6CrMwn",
            {
                "code": 0,
                "message": "",
                "data": {"position": [], "size": [{"width": 88, "height": 94}]},
            },
        ),
        (
            "2gtcy4TC4snmcMDSbTvNTZfJVGO",
            {
                "code": 0,
                "message": "",
                "data": {
                    "z": [
                        {"a": 18.444, "c": 18.444},
                        {"a": 36.888, "c": 36.888},
                        {"a": 55.333, "c": 55.333},
                        {"a": 73.777, "c": 73.777},
                        {"a": 92.221, "c": 92.221},
                        {"a": 110.665, "c": 110.665},
                        {"a": 129.11, "c": 129.11},
                        {"a": 147.554, "c": 147.554},
                        {"a": 165.998, "c": 165.998},
                        {"a": 184.442, "c": 184.442},
                        {"a": 202.886, "c": 202.886},
                        {"a": 221.331, "c": 221.331},
                        {"a": 239.775, "c": 239.775},
                        {"a": 258.219, "c": 258.219},
                        {"a": 276.663, "c": 276.663},
                        {"a": 295.107, "c": 295.107},
                        {"a": 313.552, "c": 313.552},
                        {"a": 331.996, "c": 331.996},
                        {"a": 350.44, "c": 350.44},
                        {"a": 368.884, "c": 368.884},
                    ],
                    "vv": [
                        {"a": 18.444, "c": 18.444},
                        {"a": 36.888, "c": 36.888},
                        {"a": 55.333, "c": 55.333},
                        {"a": 73.777, "c": 73.777},
                        {"a": 92.221, "c": 92.221},
                        {"a": 110.665, "c": 110.665},
                        {"a": 129.11, "c": 129.11},
                        {"a": 147.554, "c": 147.554},
                        {"a": 165.998, "c": 165.998},
                        {"a": 184.442, "c": 184.442},
                        {"a": 202.886, "c": 202.886},
                        {"a": 221.331, "c": 221.331},
                        {"a": 239.775, "c": 239.775},
                        {"a": 258.219, "c": 258.219},
                        {"a": 276.663, "c": 276.663},
                        {"a": 295.107, "c": 295.107},
                        {"a": 313.552, "c": 313.552},
                        {"a": 331.996, "c": 331.996},
                        {"a": 350.44, "c": 350.44},
                        {"a": 368.884, "c": 368.884},
                    ],
                },
            },
        ),
    ],
)
def test_manual_execute(version_id, expected_response):
    headers = {
        "blade-auth": "bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ"
        "pc3MiOiJpc3N1c2VyIiwiYXVkIjoiYXVkaWVuY2UiLCJ0ZW"
        "5hbnRfaWQiOiIwMDAwMDAiLCJsb2dpbl9pZCI6ImQ1NDk3YT"
        "I4ZjQ5MGU0OTFjMTVmNjI0ZjQ0OTViYTFjIiwidXNlcl9uYW"
        "1lIjoiYnQiLCJ0b2tlbl90eXBlIjoiYWNjZXNzX3Rva2VuIi"
        "wicm9sZV9uYW1lIjoiYWRtaW5pc3RyYXRvcixtYW5hZ2VyLH"
        "VzZXIiLCJ1c2VyX2lkIjoiM2NlMzk3MjcxYTc0M2IyMzkyYz"
        "EzMGI2YzBlZGNjMjAiLCJyb2xlX2lkIjoiMTEyMzU5ODgxNj"
        "czODY3NTIwMSwzMGI0NTcxMDYxNmQ1MTM1MzllMTVhZGVkMG"
        "NkN2QxYSwxMTIzNTk4ODE2NzM4Njc1MjAyIiwibmlja19uYW"
        "1lIjoi5rWL6K-V55So5oi3IiwiZGV0YWlsIjp7InR5cGUiOi"
        "J3ZWIifSwiZGVwdF9pZCI6ImFkYTMxNzY5OGRiMjM3NzA4MG"
        "I4YTcwYzYzMGYyNzY5IiwiYWNjb3VudCI6ImJ0IiwiY2xpZW"
        "50X2lkIjoiamt5LW1vZGVsaW5nIiwiZXhwIjoxNzE2MjU2MD"
        "gzLCJuYmYiOjE3MTYxNjk2ODN9.j2Gphu_sEd3WnayB9ta_b"
        "KKD8Ppj5iQ6mb-WOBdJWeHKBnkNYdHO9TP5PVdaS_5xwdXMO"
        "y1CUbQqXdGnQPC2KA"
    }
    body = {
        "connect_id": setting.ZMS_MAGIC,
        "version_id": version_id,
    }

    response = client.post("/pipelining/manual_execute", headers=headers, json=body)
    assert response.status_code == 200
    result = response.json()
    assert result == expected_response


@pytest.mark.parametrize(
    "version_id, pipelining_id",
    [("2hlsPNGCSwaRedc2GhKKdMMtN6U", "2hUoiSjimdhh2TxuSUj4KM8BhS7")],
)
def test_copy_version(version_id, pipelining_id):
    headers = {
        "blade-auth": "bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ"
        "pc3MiOiJpc3N1c2VyIiwiYXVkIjoiYXVkaWVuY2UiLCJ0ZW"
        "5hbnRfaWQiOiIwMDAwMDAiLCJsb2dpbl9pZCI6ImQ1NDk3YT"
        "I4ZjQ5MGU0OTFjMTVmNjI0ZjQ0OTViYTFjIiwidXNlcl9uYW"
        "1lIjoiYnQiLCJ0b2tlbl90eXBlIjoiYWNjZXNzX3Rva2VuIi"
        "wicm9sZV9uYW1lIjoiYWRtaW5pc3RyYXRvcixtYW5hZ2VyLH"
        "VzZXIiLCJ1c2VyX2lkIjoiM2NlMzk3MjcxYTc0M2IyMzkyYz"
        "EzMGI2YzBlZGNjMjAiLCJyb2xlX2lkIjoiMTEyMzU5ODgxNj"
        "czODY3NTIwMSwzMGI0NTcxMDYxNmQ1MTM1MzllMTVhZGVkMG"
        "NkN2QxYSwxMTIzNTk4ODE2NzM4Njc1MjAyIiwibmlja19uYW"
        "1lIjoi5rWL6K-V55So5oi3IiwiZGV0YWlsIjp7InR5cGUiOi"
        "J3ZWIifSwiZGVwdF9pZCI6ImFkYTMxNzY5OGRiMjM3NzA4MG"
        "I4YTcwYzYzMGYyNzY5IiwiYWNjb3VudCI6ImJ0IiwiY2xpZW"
        "50X2lkIjoiamt5LW1vZGVsaW5nIiwiZXhwIjoxNzE2MjU2MD"
        "gzLCJuYmYiOjE3MTYxNjk2ODN9.j2Gphu_sEd3WnayB9ta_b"
        "KKD8Ppj5iQ6mb-WOBdJWeHKBnkNYdHO9TP5PVdaS_5xwdXMO"
        "y1CUbQqXdGnQPC2KA"
    }
    body = {"version_id": version_id, "pipelining_id": pipelining_id}

    response = client.post("/pipelining/copy_version", headers=headers, json=body)
    assert response.status_code == 200
    assert response.json().get("code") == 0
