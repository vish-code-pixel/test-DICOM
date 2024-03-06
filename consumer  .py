import redis                        #redis connector
import pydicom                      #imaging library
import psycopg2                     #postgres connector
import matplotlib.pyplot as plt     #plotting library
import NumPy                        #NumPy is needed to work with the pixel_array property

def pop_anonymized_file_from_queue(redis_host='localhost', redis_port=6379, queue_name='anonymized_files'):
    """
    Pop a file name from the Redis LIFO queue.

    Arguments:
    - redis_host (str): The hostname or IP address of the Redis server (default: 'localhost')
    - redis_port (int): The port number of the Redis server (default: 6379)
    - queue_name (str): The name of the Redis queue (default: 'anonymized_files')

    Returns:
    - file_name (str): The name of the popped file from the Redis queue.
    """

    try:
        # Connect to Redis
        r = redis.Redis(host=redis_host, port=redis_port)

        # Pop a file name from the Redis queue (LIFO order)
        file_name = r.lpop(queue_name)

        return file_name
    
    except Exception as e:
        print(f"Error popping file from Redis queue: {e}")
        return None

def save_metadata_to_postgres(file_name, connection_string):
    """
    Extract metadata from the DICOM file and save it to a PostgreSQL database.

    Args:
    - file_name (str): The name of the DICOM file.
    - conn_string (str): The connection string for the PostgreSQL database.

    Returns:
    - None
    """
    try:
        # Load DICOM file
        dicom_dataset = pydicom.dcmread(file_name)

        # Extract metadata
        metadata = {
            'patient_name': dicom_dataset.PatientName,
            'patient_id': dicom_dataset.PatientID,
            'patient_address': dicom_dataset.PatientAddress,
            # Add more metadata fields here as needed
        }

        # Creating an object to connect to PostgreSQL database
        postgres_connector = psycopg2.connect(connection_string)

        # Creates a cursor that we will use to make the database POST request with
        cursor = postgres_connector.cursor()

        # Insert metadata into our PostgreSQL table
        cursor.execute("""
            INSERT INTO dicom_metadata (patient_name, patient_id, study_date)
            VALUES (%s, %s, %s)
        """, (metadata['patient_name'], metadata['patient_id'], metadata['patient_address']))

        # Commit the transaction
        postgres_connector.commit()

        # Close cursor and connection
        cursor.close()
        postgres_connector.close()

    except Exception as e:
        print(f"Error saving metadata to PostgreSQL: {e}")

def plot_middle_slice(file_name):
    """
    Read the DICOM file and generate a plot of the middle slice of the DICOM image using Matplotlib.

    Args:
    - file_name (str): The name of the DICOM file.

    Returns:
    - None
    """
    try:
        # Load DICOM file into our dicom_dataset object
        dicom_dataset = pydicom.dcmread(file_name)

        """
        - Gets middle slice of the dataset
        - pixel_array.shape returns a tuple representing the dimensions of the array
        - Since DICOM images can be 3D (representing volume), pixel_array.shape[0] gives the number of slices
          in the z-direction, and // 2 selects the middle slice of the dataset
        """
        middle_slice = dicom_dataset.pixel_array[dicom_dataset.pixel_array.shape[0] // 2]

        # Plot middle slice
        plt.imshow(middle_slice, cmap=plt.cm.bone)  #set the color for cmap to bone for better visualization of medical images
        plt.title('Middle Slice of DICOM Image')    
        plt.axis('off')                             #removes the extra lines and plots, cleans up the display
        plt.show()                                  #displays the actual plot
    except Exception as e:
        print(f"Error plotting middle slice of DICOM image: {e}")

if __name__ == "__main__":
    # Step 1: Pop a file name from the Redis LIFO queue
    file_name = pop_anonymized_file_from_queue()
    if file_name:
        print(f"Popped file name from Redis queue: {file_name}")

        """
        Step 2: Save metadata to PostgreSQL, we can parameterize the usn, pass to a properties file
        and base64 encode it or retrieve it from some secrets store
        """
        conn_string = "host=localhost dbname=your_database user=your_user password=your_password"
        save_metadata_to_postgres(file_name, conn_string)

        # Step 3: Generate plot of the middle slice of the DICOM image
        plot_middle_slice(file_name)
