from google.cloud import storage, vision
import re, spacy, io, os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import NullPool

def hello_gcs(data, context):
    #download spaCy within code
    # os.system("python -m spacy download en_core_web_md-3.7.1")
    
    # Get the file that has been uploaded to GCS
    bucket_name = data['bucket']
    file_name = data['name']

    # Download the image file from GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(file_name, bucket)
    blob.download_to_filename('/tmp/temp_image')

    # Open the image file
    with io.open('/tmp/temp_image', 'rb') as image_file:
        content = image_file.read()

    image = vision.Image(content=content)

    # Use Vision API to do OCR on the image
    client = vision.ImageAnnotatorClient()
    response = client.text_detection(image=image)
    texts = response.text_annotations

    # Join all the descriptions from the texts into a single string
    text_str = ' '.join(text.description for text in texts)

    # Load English tokenizer, tagger, parser, NER and word vectors
    print(spacy.cli.info())
    nlp = spacy.load("en_core_web_md")
    doc = nlp(text_str)

    # Extract email
    email_list = re.findall(r"\S+@\S+", text_str)
    email = email_list[0] if email_list else None

    # Extract phone number
    phone_list = re.findall(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text_str)
    phone = phone_list[0] if phone_list else None

    # Extract person name
    name_list = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    if name_list:
        name = ' '.join(name_list[0].split()[:2])  # Keep only the first two words
    else:
        name = None

    # Extract company
    company_list = [ent.text for ent in doc.ents if ent.label_ == "ORG"]
    company = company_list[0] if company_list else None

    print(f'Name: {name}')
    print(f'Phone: {phone}')
    print(f'Email: {email}')
    print(f'Company: {company}')

    # Database credentials
    PASSWORD = "ix[0>lhY_c#qRA=;"  # Password of DB instance on GCP
    PUBLIC_IP_ADDRESS = "104.197.69.72"  # IP address of DB instance on GCP
    DBNAME = "postgres"  # Database name on GCP, not instance name

    # Configure SQLAlchemy Database URI
    SQLALCHEMY_DATABASE_URI = f'postgresql://postgres:{PASSWORD}@{PUBLIC_IP_ADDRESS}/{DBNAME}'

    # Create engine with NullPool (to properly cleanup, otherwise conn.close puts connection into a pool)
    engine = create_engine(SQLALCHEMY_DATABASE_URI, poolclass=NullPool)

    try:
        # Initialize connection
        connection = engine.connect()

        # Connect
        result = connection.execute(text("SELECT 1"))
        print("Connection successful!")

        # Print all tables in the database
        # inspector = inspect(engine)
        # print("Tables in the database:")
        # for table_name in inspector.get_table_names():
        #     print(f"- {table_name}")

        # Print rows of a given table
        # result = connection.execute(text("SELECT * FROM example_table"))
        # for row in result:
        #     print(row)

        # Insert data into 'example_table'
        connection.execute(
            text("INSERT INTO example_table (name, email, phone, company) VALUES (:name, :email, :phone, :company)"),
            {"name": name, "email": email, "phone": phone, "company": company},
        )

        # Commit the transaction
        connection.commit()

    except Exception as e:
        print(f"Connection failed! Error: {e}")

    finally:
        if connection:
            connection.close()
            print("Connection closed")
