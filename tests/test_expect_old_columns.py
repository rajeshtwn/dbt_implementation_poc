import os
import re
import sys

# Define a list of old column names to check for
old_columns = ["LOC_KEY"]


# Function to search for references to old columns in a SQL file
def search_for_old_columns(sql_file):
    with open(sql_file, "r") as file:
        sql_code = file.read()
        for old_column in old_columns:
            if re.search(rf"\b{old_column}\b", sql_code, re.IGNORECASE):
                return True
    return False


# Function to iterate through SQL files and check for old column references
def check_old_columns():
    for root, _, files in os.walk("models"):
        for file in files:
            if file.endswith(".sql"):
                sql_file = os.path.join(root, file)
                if search_for_old_columns(sql_file):
                    return False
    return True


# Main function to run the check
def main():
    if check_old_columns():
        print("PASS")
        sys.exit(0)
    else:
        print("FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
