Hello!
I glad to see you in my solution.
So for using this application follow these steps:
1. Open your terminal make sure that port 8080 is available! 
2. Go to project folder and the docker folder inside of it.
3. Run docker compose using this command - docker-compose up -d.
4. Go to your folder with test data like product.csv and trade.csv files.
5. Run command "curl -F "file=@product.csv" http://localhost:8080/api/v1/products" to populate Redis container.
6. After step 5 run command "curl -F "file=@trade.csv" http://localhost:8080/api/v1/trades" to process the trades.

Thank you for your time!
I look forward to seeing you in a technical interview! 🎯🚀





