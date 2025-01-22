import shutil
import os
import json

def remove_gz_extension(file_name):
    if file_name.endswith(".gz"):
        return file_name[:-3]
    return file_name

def copy_and_rename_files(source_dir, destination_dir):
    os.makedirs(destination_dir, exist_ok=True)

    for file_name in os.listdir(source_dir):
        source_path = os.path.join(source_dir, file_name)
        if os.path.isfile(source_path):
            new_name = remove_gz_extension(file_name)
            destination_path = os.path.join(destination_dir, new_name)

            shutil.copy(str(source_path), str(destination_path))
            print(f"Copied and renamed: {source_path} -> {destination_path}")

def clear_directory(directory_path):
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    files_in_directory = os.listdir(directory_path)
    if files_in_directory:
        for file_name in files_in_directory:
            file_path = os.path.join(directory_path, file_name)
            if os.path.isfile(file_path):  # Check if it's a file
                os.remove(file_path)
    else:
        print(f"No files found in {directory_path}.")

def prepare_aircraft_data(directory, output_file="prepared.json", batch_size=100):
    output_path = os.path.join(directory, output_file)
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Removed existing file: {output_path}")

    all_aircraft_data = []

    for file_name in os.listdir(directory):
        if file_name.endswith(".json") and file_name != output_file:
            file_path = os.path.join(directory, file_name)

            try:
                with open(file_path, "r") as file:
                    data = json.load(file)

                aircraft_data = data.get("aircraft", [])
                aircraft_timestamp = data.get("now")
                total_records = len(aircraft_data)

                for i in range(0, total_records,batch_size):
                    batch = aircraft_data[i:i+batch_size]

                    for ac in batch:
                        aircraft = {
                            "icao": ac.get("hex"),
                            "registration": ac.get("r"),
                            "type": ac.get("t"),
                            "lat" : ac.get("lat"),
                            "lon" : ac.get("lon"),
                            "max_altitude_baro" : ac.get("alt_baro"),
                            "max_ground_speed" : ac.get("gs"),
                            "had_emergency" : ac.get("emergency"),
                            "timestamp" : aircraft_timestamp
                        }
                        all_aircraft_data.append(aircraft)

                    print(f"Processed {len(batch)} records from {file_name}")
                os.remove(file_path)

            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    with open(output_path, "w") as output_file:
        json.dump(all_aircraft_data, output_file, indent=4)
    print(f"Data saved to: {output_path}")
