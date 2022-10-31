#Deriving the latest base image
FROM python:3.7
WORKDIR src

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install Flask
RUN pip install flask_restful
COPY . .

#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

# Run our app
CMD ["python", "app.py"]
