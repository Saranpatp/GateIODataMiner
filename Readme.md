# Gate.io Crypto Data Mining (Historical Data)
Easily set up and deploy your crypto data mining application 

**NOTED:** you will need parser to be able to use with Strategy Studio.

## 🚀 Getting Started

### 🔧 Prerequisites

1. **Docker and Docker Compose**  
   Allows you to containerize and manage the application services.  
   🌐 [Download Docker](https://www.docker.com)

2. **Go** (Optional for Build from source)  
   Ensure you have the latest Go version for running locally.  
   🌐 [Download Go](https://golang.org/dl/)

## Author

### **Saranpat Prasertthum (saranpatp@gmail.com)**

* My name is **Saranpat Prasertthum**, and I am currently pursuing my **Master's in Financial Engineering** at the University of Illinois at Urbana-Champaign, with an expected graduation in December 2023. My academic journey has equipped me with knowledge in High Frequency Trading, Stochastic Calculus, and Algorithmic Trading. Professionally, I spent **two years as a Software Engineer** at Blockfint, a startup company. I am proficient in languages such as **Python, C++, Go, Java, and JavaScript, and have a strong background in SQL**. Additionally, I have hands-on experience as a **backend developer** and am well-versed in cloud platforms. My areas of interest encompass **high-performance computing, finance, and trading**. My Linkedin is located at: **https://www.linkedin.com/in/saranpatp**


---

## 📦 Deployment

This section guides you through deploying the application either using Docker or by running it locally on your machine.

### Initial Setup

Before proceeding with either deployment method, you need to create and configure a `.env` file in the project directory. This file should contain the configurations for fetching historical data.

Create a `.env` file with the following content:
```
START_DATE=2022-01-01
END_DATE=2023-01-01
MARKETS=BTC_USDT,ETH_USDT,XRP_USDT
```
- `START_DATE` and `END_DATE` define the date range for the historical data.
- `MARKETS` lists the cryptocurrency pairs you want to fetch data for, separated by commas.

### Using Docker

Docker simplifies deployment by containerizing the application and its environment. Follow these steps to deploy using Docker:

1. **Build and Start the Containers**  
   Navigate to the project's root directory and run the following commands to build and start the Docker containers:
   ```bash
   docker-compose build
   docker-compose up -d
   ```
   - `docker-compose build` will build the Docker images.
   - `docker-compose up -d` will start the containers in detached mode.

2. **Verify the Deployment**  
   After executing the above commands, ensure that the containers are running correctly. You can check the status of the containers using:
   ```bash
   docker-compose ps
   ```

### Running Locally (Recommended)

If you prefer to run the application directly on your local machine, follow these steps:

1. **Install Necessary Go Packages**  
   The application requires certain Go packages to run. Install them by executing the following command in the root directory of the project:
   ```bash
   go get
   ```
   This command fetches and installs the dependencies listed in the `go.mod` file.

2. **Start the Application**  
   Once all dependencies are installed, start the application by running:
   ```bash
   ./run.sh
   ```
   This command compiles and executes the `parser/parser.go` and `downloader/downloader.go` files which will start downloading and parsing the data.

