# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/app

# Copy the current directory contents into the container at /usr/app
COPY . /usr/app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir dbt-postgres

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variables
ENV DBT_PROFILES_DIR=/usr/app

# Run dbt when the container launches
CMD ["dbt", "--version"]