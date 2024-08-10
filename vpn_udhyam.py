import time
import csv
import base64
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.request
import pymysql
import requests
import subprocess
import random
import os

# CHROME_DRIVER_PATH = r'C:\Program Files\chromedriver-win32\chromedriver.exe'  # Path to your ChromeDriver
CSV_FILE_PATH = '10Lacs.csv'  # Path to your CSV file
OUTPUT_CSV_FILE = 'extracted_details_final.csv'  # Output CSV file to save the details
STATUS_CSV_FILE = 'status.csv'
TRACKER_CSV_FILE = 'resume.csv'

# VPN CODE #
def get_public_ip(max_retries=5, delay=10):
    urls = ["https://api.ipify.org?format=json", "https://api.my-ip.io/ip"]
    for attempt in range(max_retries):
        for url in urls:
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.json()['ip']
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                time.sleep(delay)
    raise Exception("Failed to retrieve public IP after several attempts.")


def execute_command(command):
    try:
        result = subprocess.run(
            command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode().strip()
    except subprocess.CalledProcessError as e:
        raise Exception(f"Command failed: {e}")


def get_vpn_list():
    try:
        result = execute_command("expressvpn list all")
        lines = result.strip().split("\n")
        aliases = [line.split()[0] for line in lines if line.strip().endswith("Y")]
        return aliases[:10]
    except Exception as e:
        raise Exception(f"Failed to retrieve VPN list: {str(e)}")


def disconnect_from_vpn():
    try:
        current_ip = get_public_ip()
        print(f'Current IP before disconnecting VPN: {current_ip}')
        print('Disconnecting VPN...')
        execute_command("expressvpn disconnect")
        print(f"Disconnected VPN from IP: {current_ip}")
    except Exception as e:
        print(f"Error: {str(e)}")


def connect_to_vpn():
    try:
        aliases = get_vpn_list()
        if not aliases:
            print("No VPN servers available.")
            return
        selected_alias = random.choice(aliases)
        print(f"Connecting to VPN {selected_alias}...")
        execute_command(f"expressvpn connect {selected_alias}")
        time.sleep(15)  # Wait for VPN connection to establish
        new_ip = get_public_ip()
        print(f"Connected to VPN with IP: {new_ip}")
    except Exception as e:
        print(f"Error: {str(e)}. Retrying VPN connection...")
        time.sleep(10)  # Wait before retrying
        connect_to_vpn()


def reconnect_vpn_and_reinitialize_driver(driver):
    print("Changing VPN connection...")
    disconnect_from_vpn()
    connect_to_vpn()
    driver.quit()
    driver = initialize_driver()
    return driver


# Most helpful when you got erroring chrome driver path
def find_chromedriver_path():
    try:
        result = subprocess.run(['which', 'chromedriver'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        path = result.stdout.decode().strip()
        if not path:
            raise FileNotFoundError("ChromeDriver not found in PATH.")
        return path
    except Exception as e:
        raise Exception(f"Error finding ChromeDriver path: {e}")

def initialize_driver():
    options = Options()
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-infobars')
    # options.add_argument('--headless')  # Headless mode for faster operation
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    # Uncomment if you encounter issues on certain systems
    # options.add_argument("--no-sandbox") 

    #driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=options)
    #return driver
    CHROME_DRIVER_PATH = find_chromedriver_path()
    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=options)

    # Set the window size
    driver.set_window_size(1920, 1080)

    return driver
# VPN CODE #



def image_to_base64(image_data):
    # Convert image data to base64
    return base64.b64encode(image_data).decode('utf-8')

def get_ocr(captcha_image_base64):
    url = "https://api.apitruecaptcha.org/one/hello"
    userid = "nishit@egniol.co.in"
    api_key = "NleeCfuixkeWXvQBByMB"
    
    payload = {
        'data': captcha_image_base64,
        'client': 'chrome extension',
        'userAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36',
        'location': 'https://truecaptcha.org/',
        'version': '0.3.8',
        'case': 'upper',
        'promise': 'true',
        'extension': True,
        'userid': userid,
        'apikey': api_key
    }
        
    headers = {
        'Host': 'api.apitruecaptcha.org',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Origin': 'https://truecaptcha.org/',
        'Referer': 'https://truecaptcha.org/'
    }
        
    response = requests.post(url, headers=headers, json=payload)
    return response.json()

def save_to_csv(data, output_file):
    # Define the header based on the keys in the data
    header = data[0].keys()
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)


def convert_date(date_str):
    if date_str:
        try:
            return time.strftime('%Y-%m-%d', time.strptime(date_str, '%d/%m/%Y'))
        except ValueError:
            print(f"Date format error for {date_str}")
            return None
    return None

def write_to_csv(data, output_file='output.csv'):
    # Ensure there's data to write
    if not data:
        print("No data to write.")
        return
    
    # Extract the header from the keys of the first dictionary
    header = data[0].keys()
    
    # Open the CSV file for appending
    with open(output_file, 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        
        # Write the header only if the file is empty
        if file.tell() == 0:
            writer.writeheader()
        
        # Write the data
        writer.writerows(data)


def enter_udyam_number(driver, udyam_number):
    try:
        # Wait until the Udyam number input field is visible
        wait = WebDriverWait(driver, 10)
        udyam_input = wait.until(EC.visibility_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtUdyamNo")))
        udyam_input.clear()
        udyam_input.send_keys(udyam_number)
            
        # Get CAPTCHA image URL
        captcha_image_element = wait.until(EC.visibility_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_imgCaptcha")))
            
            # Download and convert CAPTCHA image to base64
        if captcha_image_element:
            captcha_base64 = captcha_image_element.screenshot_as_base64
            # Print CAPTCHA base64 for debugging
            print(f"Captcha Base64: {captcha_base64[:100]}...")  # Print first 100 characters for debugging
            
            # Get CAPTCHA code using OCR
            ocr_result = get_ocr(captcha_base64)
            print(f"OCR Result: {ocr_result}")  # Print OCR result for debugging
            
            # Extract CAPTCHA code from OCR result
            captcha_code = ocr_result.get('result', '')
            print(f"Captcha Code: {captcha_code}")  # Print CAPTCHA code for debugging
            
            if not captcha_code:
                print("Failed to get CAPTCHA code.")
                return None
            
            # Print CAPTCHA code for debugging
            print(f"Captcha Code: {captcha_code}")
            
            # Enter CAPTCHA code
            captcha_input = wait.until(EC.visibility_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtCaptcha")))
            captcha_input.clear()
            captcha_input.send_keys(captcha_code)
            
            # Submit the form
            submit_button = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnVerify")))
            submit_button.click()

            # Wait for the response to be visible
            time.sleep(15)  # Adjust if needed for the response to be visible

            # Extract details
            details = {'Udyam Registration Number': udyam_number}

            # Extract details from the page
            try:
                details['Enterprise Name'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblEnterpriseName").text
            except Exception:
                details['Enterprise Name'] = None

            try:
                details['Organisation Type'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblOrganisationType").text
            except Exception:
                details['Organisation Type'] = None

            try:
                details['Major Activity'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblServices").text
            except Exception:
                details['Major Activity'] = None

            try:
                details['Gender'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblGender").text
            except Exception:
                details['Gender'] = None

            try:
                details['Social Category'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblsocialcat").text
            except Exception:
                details['Social Category'] = None

            try:
                details['Date of Incorporation'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lbldateofincorporation").text
            except Exception:
                details['Date of Incorporation'] = None

            try:
                details['Date of Commencement'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lbldateofcommencement").text
            except Exception:
                details['Date of Commencement'] = None

            # Official address details
            try:
                details['Flat/Door/Block No.'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblflats").text
            except Exception:
                details['Flat/Door/Block No.'] = None

            try:
                details['Name of Premises/ Building'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblBuilding").text
            except Exception:
                details['Name of Premises/ Building'] = None

            try:
                details['Village/Town'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblVillage").text
            except Exception:
                details['Village/Town'] = None

            try:
                details['Block'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblBlock").text
            except Exception:
                details['Block'] = None

            try:
                details['Road/Street/Lane'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblRoad").text
            except Exception:
                details['Road/Street/Lane'] = None

            try:
                details['City'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblCity").text
            except Exception:
                details['City'] = None

            try:
                details['State'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblState").text
            except Exception:
                details['State'] = None

            try:
                details['District'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblDistrict").text
            except Exception:
                details['District'] = None

            try:
                details['Pin'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblPin").text
            except Exception:
                details['Pin'] = None

            try:
                details['Mobile'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblMobile").text
            except Exception:
                details['Mobile'] = None

            try:
                details['Email'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblEmail").text
            except Exception:
                details['Email'] = None

            # DIC and MSME-DFO details
            try:
                details['DIC'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblgmdic").text
            except Exception:
                details['DIC'] = None

            try:
                details['MSME-DFO'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblMSMEDI").text
            except Exception:
                details['MSME-DFO'] = None

            try:
                details['Date of Udyam Registration'] = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblACKNOWLEDGEMENT").text
            except Exception:
                details['Date of Udyam Registration'] = None

            # Click the "Back" button
            back_button = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnBack")))
            back_button.click()
                
            return details

    except Exception as e:
        print(f"An error occurred while processing Udyam number {udyam_number}: {e}")
        return None


def main():
    # connect_to_vpn()
    driver = initialize_driver()
    
    url = "https://udyamregistration.gov.in/Government-India/Ministry-MSME-registration.htm"
    driver.get(url)

    extracted_data = []

    queries_since_last_vpn_change = 0  # Initialize the counter
    try:
        # Wait until the dropdown is clickable
        wait = WebDriverWait(driver, 10)
        dropdown_menu = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Print / Verify")))
        dropdown_menu.click()

        # Wait until the "Verify Udyam Registration Number" link is clickable
        verify_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Verify Udyam Registration Number")))
        verify_link.click()

        # Optionally, wait for the new page to load
        time.sleep(15)  # Adjust the wait time as needed for the page to load

        # Prepare the CSV file with headers
        with open('output.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['udyam_registration_number', 'enterprise_name', 'organisation_type', 'major_activity',
                'gender', 'social_category', 'date_of_incorporation', 'date_of_commencement', 'flat_door_block_no',
                'name_of_premises_building', 'village_town', 'block', 'road_street_lane', 'city', 'state', 'district', 'pin',
                'mobile', 'email', 'dic', 'msme_dfo', 'date_of_udyam_registration'])

        # Read Udyam numbers from CSV and enter them
        all_data = []
        with open(CSV_FILE_PATH, newline='') as csvfile:
            reader = csv.DictReader(csvfile)  # Use DictReader to access columns by name

            for row in reader:
                udyam_number = row['Name']
                details = enter_udyam_number(driver, udyam_number)
                    
                if details:
                    all_data.append(details)
                    with open(STATUS_CSV_FILE, 'a', newline='') as status_csv:
                        status_writer = csv.writer(status_csv)
                        status_writer.writerow([udyam_number, 'Success'])
                    write_to_csv([details]) 
                    print(f"Successfully processed: {udyam_number}")
                else:
                    with open(STATUS_CSV_FILE, 'a', newline='') as status_csv:
                        status_writer = csv.writer(status_csv)
                        status_writer.writerow([udyam_number, 'Failed'])
                    print(f"Failed to process: {udyam_number}")
                      
                # queries_since_last_vpn_change += 1
                if queries_since_last_vpn_change >= 100:
                    print("Changing VPN connection...")
                    driver = reconnect_vpn_and_reinitialize_driver(driver) 
                    queries_since_last_vpn_change = 0

                time.sleep(2)  # Adjust the delay as needed
    finally:
        driver.quit()
        # disconnect_from_vpn()
        
        # Save the extracted data to a CSV file
    save_to_csv(extracted_data, OUTPUT_CSV_FILE)

if __name__ == "__main__":
    main()