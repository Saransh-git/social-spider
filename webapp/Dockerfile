FROM python:3.7.0
RUN mkdir /src
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt
EXPOSE 8000
RUN python manage.py check
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
