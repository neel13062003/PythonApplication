from selenium import webdriver
from selenium.webdriver.common.keys import Keys

# Set up the browser driver (assuming Chrome)
driver = webdriver.Chrome()

# Navigate to the login page
driver.get("https://ddu.gnums./Login.aspx")

# Find the username and password input fields and fill them with test data
username_field = driver.find_element("id", "txtUsername")  # Updated here
username_field.send_keys("vipuldabhi.it@ddu.ac.in")
password_field = driver.find_element("id", "txtPassword")  # Updated here
password_field.send_keys("vipul@312")

# Find and submit the login form
login_button = driver.find_element("id", "btnLogin")  # Updated here
login_button.click()

# Close the browser
#driver.quit()
