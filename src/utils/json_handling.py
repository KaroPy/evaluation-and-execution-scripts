import json


def write_data_to_json(json_data, file_name: str):
    with open(f"{file_name}.json", "w") as f:
        json.dump(json_data, f, indent=2)


def read_data_from_json(file_name: str):
    with open(f"{file_name}.json", "r") as f:
        return json.load(f)
