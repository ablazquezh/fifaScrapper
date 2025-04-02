import os
import urllib.parse

# Path to the directory containing the images
image_directory = 'C:\\Users\\albbl\\Desktop\\projects\\fantasymbeciles\\public\\static\\teams\\fifa19'  # Adjust this path if needed

# Function to rename the files with decoded names
def decode_filenames_in_directory(directory):
    # Loop through all files in the specified directory
    for filename in os.listdir(directory):
        # Full path to the file
        old_path = os.path.join(directory, filename)
        old_name = filename
        if "b" == filename[0]:
            print(filename)
            filename = filename[1:]
            filename = filename.replace("'", "")
            #old_path = os.path.join(image_directory, filename)
            
            print(f"Renamed:   {old_name} ->{filename}")

        # Check if it's a file (not a directory)
        if os.path.isfile(old_path):
            # Decode the filename (URL decoding)
            decoded_filename = urllib.parse.unquote(filename)
            
            # If the filename is the same as the decoded one, no need to rename
            if decoded_filename != filename:
                new_path = os.path.join(directory, decoded_filename)

                # Rename the file to the decoded filename
                try:
                    os.rename(old_path, new_path)
                except: 
                    os.remove(old_path)
                #print(f"Renamed: {filename} -> {decoded_filename}")
        elif isinstance(filename, bytes):  # Check if it's a byte object
            decoded_filename = filename.decode("utf-8")
            old_path = os.path.join(image_directory, filename)
            new_path = os.path.join(image_directory, decoded_filename)
            
            try:
                os.rename(old_path, new_path)
            except: 
                os.remove(old_path)
            #print(f"Renamed: {filename} -> {decoded_filename}")
        else:
            print(f"Skipped (no decoding needed): {filename}")

# Run the function to decode and rename files
decode_filenames_in_directory(image_directory)


# Function to rename the files with decoded names
def decode_filenames_in_directory2(directory):
    # Loop through all files in the specified directory
    for filename in os.listdir(directory):
        if "b" == filename[0]:
            print(filename)
            newname = filename.replace("b", "")
            newname = newname.replace("'", "")
            old_path = os.path.join(image_directory, filename)
            
            """try:
                os.rename(old_path, os.path.join(image_directory, newname))
            except:
                os.remove(old_path)"""
            print(f"Renamed: {filename} -> {newname}")