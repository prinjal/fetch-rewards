# Dockerfile

FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

COPY ./data /data

# Copy the rest of the application
COPY . .

# Set the entrypoint to the load_data.py script
ENTRYPOINT ["python", "load_elt.py"]
