Hello!
In my solution I implemented all steps.

So for using this application follow these steps:
1. Open project folder and run "mvn clean compile package"
2. Go to docker folder.
3. Run docker compose using this command "docker-compose up -d"
4. Go to your folder with test data like product.csv and trade.csv files.
5. Run command "curl -F "file=@product.csv" http://localhost:8080/api/v1/products" to populate Redis container.
6. After step 5 run command "curl -F "file=@trade.csv" http://localhost:8080/api/v1/trades" to process the trades.


Tests screenshot - https://drive.google.com/file/d/1XV0fllxEO8gPSIVE7aA3QVtjCNUMmt4o/view?usp=sharing

Thank you for your time!
I look forward to seeing you in a technical interview! 🎯🚀

