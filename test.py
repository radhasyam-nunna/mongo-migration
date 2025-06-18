import json

# Path to the JSON file
file_path = "output.json"

# List to store appointmentIds
appointment_ids = []

# Read the JSON file line by line
with open(file_path, 'r') as file:
    for line in file:
        data = json.loads(line.strip())
        if 'appointmentId' in data:
            appointment_ids.append(data['appointmentId'])

# Print all extracted appointmentIds
# for aid in appointment_ids:
#     print(aid)

print(len(appointment_ids)," ",len(set(appointment_ids)))

# Optionally, write them to a text file
with open("appointment_ids.txt", "w") as outfile:
    for aid in appointment_ids:
        outfile.write(aid + "\n")
