To Run the Application perform the following steps:

1. docker-compose up    #run the docker compose yaml file 


2. docker ps            ## get the <container_id> for the currently running instance of the Nifi image

3. you can upload the XML file (CSV_to_JSON.xml) as a template to load my workflow nodes to Nifi.

4. After running the Nifi workflow you can get into the mounted output folder and make sure the covid19.json file is present   

5. docker-compose down #terminate the application