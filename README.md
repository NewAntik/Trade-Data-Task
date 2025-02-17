Hello!
In my solution I implemented all steps.
If I had more time, I’d investigate how to implement a reactive approach using best practices.


So for using this application follow these steps:

1. Open project folder and run "mvn clean compile package"
2. Go to docker folder.
3. Run docker compose using this command "docker-compose up -d"
4. Go to your folder with test data like product.csv and trade.csv files.
5. Run command "curl -F "file=@product.csv" http://localhost:8080/api/v1/products" to populate Redis container.
6. After step 5 run command "curl -F "file=@trade.csv" http://localhost:8080/api/v1/trades" to process the trades.


I've tested the program using product.csv file(2,2 MB) and trade.csv file(4,2mb)
As results: https://drive.google.com/drive/folders/1yB8g_m2twytaYDqZUmEKjbxrvsWL8iU4?usp=sharing


Thank you for your time!
I look forward to seeing you in a technical interview! 🎯🚀

