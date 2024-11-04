import smtplib

def send():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login("sidiqfaisal30@gmail.com", "juva yapw wakj apxt")
        subject="Testing"
        body="Testing Success"
        msg="Subject:{}\n\n{}".format(subject,body)
        x.sendmail("sidiqfaisal30@gmail.com","mfaisalshidiq@gmail.com",msg)
        print("Successfully sent email")
    except Exception as exception:
        print(exception)
        print("Failure to send email")
    

if __name__ == "_main_":
    send()