import pandas as pd
import ast, csv  # Library to parse strings as Python literals

# Read the CSV file and format the 'artists' and 'artist_ids' columns
with open('tracks_features.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)

    # Loop through the rows and format the 'artists' and 'artist_ids' columns
    for row in data:
        # Convert 'artists' column from string to list
        row['artists'] = ast.literal_eval(row['artists'])

        # Convert 'artist_ids' column from string to list
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])

df = pd.DataFrame(data)

# Export DataFrame to JSON file
df.to_json('dataframe.json', orient='records', indent=4)