import time
import random
from selenium import webdriver
import selenium.common.exceptions as exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import boto3
from io import StringIO
from Keys import KEY_ID, SECRET_KEY


def connect_to_indeed():
    global driver
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("detach", True)

    try:
        driver = webdriver.Chrome(options=options,
                                  executable_path=r"C:\Chrome_Driver\chromedriver.exe")
    except exceptions.WebDriverException:
        print("Chrome driver is not available.")
    try:
        driver.get("https://ca.indeed.com/jobs?q=full+time&l=Canada")
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Find jobs']")))
    except ImportError:
        print("Cannot open website")


def set_options():
    driver.find_element(By.ID, "filter-dateposted").click()
    time.sleep(1)
    driver.find_element(By.XPATH, "//a[text()='Last 24 hours']").click()
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, ".popover-x-button-close.icl-CloseButton").click()
    time.sleep(1)
    driver.find_element(By.ID, "filter-taxo1").click()
    time.sleep(1)
    driver.find_element(By.XPATH, "//a[contains(text(), 'Technology Occupations')]").click()


def get_page_numbers():
    temp_url = driver.current_url
    driver.get(f'{temp_url}&start=999999')
    number = driver.find_element(By.XPATH, "//b[@aria-label]").text
    time.sleep(3)
    driver.get(temp_url)
    time.sleep(3)
    print('Need to scrape ' + str(number) + ' pages')
    return number


def scrape_job_info(pages):
    job_list = []
    temp_url = driver.current_url
    for i in range(int(pages)):
    # for i in range(1):
        driver.get(f'{temp_url}&start={str(i)}0')
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for info in soup.find_all('div', class_='slider_item'):
            temp_dict = {}
            temp_dict['title'] = info.find('h2').getText().strip("new")
            temp_dict['company'] = info.find('span', {'class': 'companyName'}).getText()
            if info.find('span', {'class': 'ratingNumber'}):
                temp_dict['rating'] = info.find('span', {'class': 'ratingNumber'}).getText()
            else:
                temp_dict['rating'] = 'N/A'
            if '•' in info.find('div', {'class': 'companyLocation'}).getText():
                temp_dict['location'] = info.find('div', {'class': 'companyLocation'}).getText().split('•')[0]
                temp_dict['remote'] = info.find('div', {'class': 'companyLocation'}).getText().split('•')[1]
            else:
                temp_dict['location'] = info.find('div', {'class': 'companyLocation'}).getText()
                temp_dict['remote'] = 'On-Site'
            if info.find('div', {'class': 'salary-snippet'}):
                temp_dict['salary'] = info.find('div', {'class': 'salary-snippet'}).getText()
            else:
                temp_dict['salary'] = 'N/A'
            temp_dict['date'] = str(date.today())
            job_list.append(temp_dict)
        time.sleep(random.randint(5, 15))
    return job_list


def data_transform(job_list):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET_KEY
    )
    job_df = pd.DataFrame(job_list)
    bucket = s3.Bucket('wcd-landing-zone')
    csv_buffer = StringIO()
    job_df.to_csv(csv_buffer, header=True, index=False)
    file_name = "jobs_" + str(date.today()) + ".csv"
    s3.Object(bucket.name, file_name).put(Body=csv_buffer.getvalue())


if __name__ == '__main__':
    connect_to_indeed()
    set_options()
    page_number = get_page_numbers()
    jobs = scrape_job_info(page_number)
    data_transform(jobs)
