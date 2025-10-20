def compare_files(file1_path, file2_path):
    try:
        # Read the content of the files
        with open(file1_path, 'r') as file1:
            file1_content = set(file1.readlines())
        
        with open(file2_path, 'r') as file2:
            file2_content = set(file2.readlines())
        
        # Find differences
        differences = list(file1_content.symmetric_difference(file2_content))
        
        return differences
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# Example usage
file1_path = r'C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\61054excel.txt'
file2_path = r'C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\61054sql.txt'

differences = compare_files(file1_path, file2_path)

print(f"Number of differences: {len(differences)}")
for diff in differences:
    print(diff.strip())