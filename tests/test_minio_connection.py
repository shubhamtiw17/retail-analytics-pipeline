import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from minio import Minio
from minio.error import S3Error
import io

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

try:
    # Test write
    data = b"hello from python"
    client.put_object(
        "retail-lake",
        "bronze/_connection_test/test.txt",
        io.BytesIO(data),
        length=len(data)
    )
    print("✅ MinIO write succeeded")

    # Test read
    response = client.get_object("retail-lake", "bronze/_connection_test/test.txt")
    content = response.read()
    print(f"✅ MinIO read succeeded — content: {content.decode()}")

except S3Error as e:
    print(f"❌ MinIO error: {e}")