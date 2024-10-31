# import modules
import base64
import datetime
import os
from datetime import *

import oracledb
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.message import EmailMessage

DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to
print(f"Username: {DB_UN} | Password: {DB_PW} | Server: {DB_CS}")  # debug so we can see where oracle is trying to connect to/with

# Google API Scopes that will be used. If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

SCHOOL_CODES = ['5','1003', '1004']
IGNORE_FULLYEAR_TERMS = False
TEACHER_ROLE_NAMES = ['Lead Teacher', 'Co-teacher']  # the role names of the teachers that we want to include in the emails. These are found in roledef.name
ATTENDANCE_CODES = {'AB': 'Excused Absence Full Day', 'UN': 'Unexcused Absence Full Day', 'UA': 'Unexcused Absence Full Day', 'SS': 'Suspension', 'ASP': 'Alternative Study Program', 'ISS': 'In School Study'}

if __name__ == '__main__':  # main file execution
    with open('ath_abs_notifs_log.txt', 'w') as log:  # open logging file
        startTime = datetime.now()
        today = datetime.now()
        todaysDate = datetime.now()
        todaysDate = todaysDate.replace(hour=0, minute=0, second=0, microsecond=0)  # get the date without any timecode as all daily attendance codes have just the date and no time
        startTime = startTime.strftime('%H:%M:%S')
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)

        try:   # Do the Gmail API authentication flow
            creds = None
            # The file token.json stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
            if os.path.exists('token.json'):
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
            service = build('gmail', 'v1', credentials=creds)  # create the Google API service with just gmail functionality
        except Exception as er:
            print(f'ERROR while doing initial Google API authentication flow: {er}')
            print(f'ERROR while doing initial Google API authentication flow: {er}', file=log)

        # Start the actual DB queries
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
            with con.cursor() as cur:  # start an entry
                for schoolCode in SCHOOL_CODES:
                    termlist = []  # make an empty list for valid terms per building
                    try:
                        termid = None
                        cur.execute("SELECT id, firstday, lastday, schoolid, dcid, isyearrec FROM terms WHERE schoolid = :school ORDER BY dcid DESC", school=schoolCode)  # get a list of terms for the school, filtering to not full years
                        terms = cur.fetchall()
                        for term in terms:  # go through every term
                            termStart = term[1]
                            termEnd = term[2]
                            isYear = term[5]
                            #compare todays date to the start and end dates
                            if ((termStart < today) and (termEnd > today)):
                                termid = str(term[0])
                                termDCID = str(term[4])
                                if isYear == 1 and not IGNORE_FULLYEAR_TERMS:  # if the term we found is marked as a full year term and we arent ignoring them
                                    print(f'DBUG: Found yearlong term at building {schoolCode}: {termid} | {termDCID}')
                                    print(f'DBUG: Found yearlong term at building {schoolCode}: {termid} | {termDCID}', file=log)
                                    termlist.append(termid)  # add the term to the list we will iterate through. Comment out to ignore yearlong terms or set IGNORE_FULLYEAR_TERMS to True
                                else:
                                    print(f'DBUG: Found good term at building {schoolCode}: {termid} | {termDCID}')
                                    print(f'DBUG: Found good term at building {schoolCode}: {termid} | {termDCID}', file=log)
                                    termlist.append(termid)  # add the term to the list we will iterate through. Comment out to ignore quarters, semesters, etc
                    except Exception as er:
                        print(f'ERROR while finding current term in building {schoolCode}: {er}')
                        print(f'ERROR while finding current term in building {schoolCode}: {er}', file=log)
                    # go through all the valid terms we found
                    for termid in termlist:
                        print(f'DBUG: Starting term {termid} at building {schoolCode}')
                        print(f'DBUG: Starting term {termid} at building {schoolCode}', file=log)
                        courseList = []  # make an empty list that will contain the course dicts inside it and student dict
                        courseDict = {}
                        try:  # find all courses with ath (athletics) or act (activities) in the name
                            cur.execute("SELECT course_number, course_name FROM courses WHERE (instr(course_name, 'ATH-') > 0 OR instr(course_name, 'ACT-') > 0)")
                            courses = cur.fetchall()
                            for course in courses:
                                try:
                                    courseNum = course[0]
                                    courseName = course[1]
                                    # print(course)  # debug
                                    # next find all students in the current course in the currend term
                                    cur.execute('SELECT students.student_number, students.first_name, students.last_name, cc.sectionid FROM cc LEFT JOIN students ON cc.studentid = students.id WHERE cc.course_number = :course AND cc.termid = :term AND cc.schoolid = :school', course=courseNum, term=termid, school=schoolCode)
                                    students = cur.fetchall()
                                    sectionsDict = {}  # dict for each section that will contain section ID, student list, and teachers info
                                    for student in students:
                                        try:
                                            # print(student)
                                            studentNum = str(int(student[0]))
                                            studentFirst = student[1]
                                            studentLast = student[2]
                                            studentName = f'{studentFirst} {studentLast}'
                                            sectionID = student[3]  # section ID for the section that student is enrolled in
                                            # print(f'DBUG: Starting student {studentNum} in section {sectionID}')  # debug
                                            if not sectionsDict.get(sectionID):  # the first time we get a new section, need to initialize its sub-dicts
                                                # print(f'DBUG: No entry exists in the dict for section ID {sectionID}, initializing it')  # debug
                                                sectionsDict.update({sectionID: {'Teachers': {}, 'Students': {}}})
                                            # print(f'DBUG: Current sectionsDict looks like: {sectionsDict}')  # debug
                                            studentDict = sectionsDict.get(sectionID).get('Students')  # get the current student list
                                            studentDict.update({studentNum: studentName})  # add the student to the list of students in the current section
                                            try:
                                                # print(f'DBUG: Curent teachers sub-dict: {sectionsDict.get(sectionID).get('Teachers')}')  # debug
                                                if not sectionsDict.get(sectionID).get('Teachers').keys():  # if we dont have any entries in the teachers list, we need to find them
                                                    # print(f'DBUG: No teacher info found for section {sectionID}, finding them')  # debug
                                                    teacherDict = {}
                                                    # find the teachers and co-teachers for the section
                                                    cur.execute('SELECT u.email_addr, u.lastfirst, rd.name FROM sectionteacher st LEFT JOIN roledef rd ON st.roleid = rd.id LEFT JOIN schoolstaff staff ON st.teacherid = staff.id LEFT JOIN users u ON staff.users_dcid = u.dcid WHERE st.sectionid = :section', section=sectionID)
                                                    teachers = cur.fetchall()
                                                    for teacher in teachers:
                                                        if teacher[2] in TEACHER_ROLE_NAMES:  # only add them to the teacher dict if they are one of the roles we want to notify
                                                            if not teacherDict.get(teacher[0]):  # if they dont already exist in the teacher dict
                                                                teacherDict.update({teacher[0]: teacher[1]})
                                                else:
                                                    teacherDict = sectionsDict.get(sectionID).get('Teachers')
                                            except Exception as er:
                                                print(f'ERROR while finding teacher dictionary or teacher info for section {sectionID} of course {courseName}: {er}')
                                                print(f'ERROR while finding teacher dictionary or teacher info for section {sectionID} of course {courseName}: {er}', file=log)

                                            sectionsDict.update({sectionID: {'Students': studentDict, 'Teachers': teacherDict}})  # do the update with the new student and teacher info (if applicble) to the sections dict
                                        except Exception as er:
                                            print(f'ERROR while processing student {student[0]} in section {sectionID} of course {courseName}: {er}')
                                            print(f'ERROR while processing student {student[0]} in section {sectionID} of course {courseName}: {er}', file=log)
                                    if sectionsDict:  # if we found any sections with students in them, update the overall course dict with that section sub-dict
                                        # print(sectionsDict)  # debug
                                        courseDict.update({courseName: sectionsDict})
                                except Exception as er:
                                    print(f'ERROR while finding sections of course {courseName}: {er}')
                                    print(f'ERROR while finding sections of course {courseName}: {er}', file=log)
                            # print(courseDict)  # debug
                            # now that we have found the courses, sections for those courses, and student and teacher info in each section, we look for absences of students per section and send emails to the section teacher
                            for activity in courseDict.keys():  # get the list of course names and go through them one at a time
                                try:
                                    activityPrefix = {'ACT': 'Activity: ', 'ATH': 'Athletics: '}
                                    activityProper = activityPrefix.get(activity.split('-')[0]) + activity.split('-')[1].title()
                                    print(f'DBUG: Starting activity {activityProper}')
                                    sections = courseDict.get(activity).keys()
                                    for section in sections:  # go through each section of the current activity
                                        try:
                                            absenceList = []  # create an empty list each section that will store which students have actuall had an absence
                                            toEmail = ''  # set the to email blank each new section
                                            print(f'INFO: Found section for activity {activity} with ID {section}')
                                            print(f'INFO: Found section for activity {activity} with ID {section}', file=log)
                                            teacherEmails = courseDict.get(activity).get(section).get('Teachers').keys()
                                            # append all the emails into a single string that will be used in the "To" field for the email
                                            for email in teacherEmails:  
                                                if toEmail == '':
                                                    toEmail = email
                                                else:
                                                    toEmail += f', {email}'
                                            print(f'DBUG: Emails for section {section} in activity {activity} will be sent to "{toEmail}"')
                                            print(f'DBUG: Emails for section {section} in activity {activity} will be sent to "{toEmail}"', file=log)
                                            students = courseDict.get(activity).get(section).get('Students').items()  # get the student number and name pairings as tuples
                                            for studentNum, studentName in students:
                                                try:
                                                    # print(f'DBUG: Looking for absences for student number {studentNum} - {studentName}')
                                                    cur.execute('SELECT att.att_code FROM ps_attendance_daily att LEFT JOIN students s ON s.id = att.studentid WHERE s.student_number = :student AND att.schoolid = :school AND att.att_date = :today', student=studentNum, school=schoolCode, today=todaysDate)
                                                    absences = cur.fetchall()
                                                    for absence in absences:
                                                        absenceCode = absence[0]
                                                        print(f'DBUG: Student {studentNum} has an absence with code {absenceCode}')  # debug
                                                        print(f'DBUG: Student {studentNum} has an absence with code {absenceCode}', file=log)  # debug
                                                        if absenceCode in ATTENDANCE_CODES.keys():  # if the attendance code matches one of the ones we are looking for
                                                            absenceList.append(f'{studentNum} - {studentName}: {ATTENDANCE_CODES.get(absenceCode)}\n')  # add the student number and number as well as the absence code in a single string to the absence list
                                                except Exception as er:
                                                    print(f'ERROR while finding absences for student {studentNum} in section {section} of activity {activity}: {er}')
                                                    print(f'ERROR while finding absences for student {studentNum} in section {section} of activity {activity}: {er}', file=log)
                                            if absenceList:  # if there are any absences
                                                print(f'INFO: Sending emails about absences for the following students: {absenceList}')
                                                print(f'INFO: Sending emails about absences for the following students: {absenceList}', file=log)
                                                absenceString = ''
                                                for entry in absenceList:
                                                    absenceString += entry
                                                # print(absenceString)
                                                todaysDatecode = datetime.now().strftime('%m/%d/%y')
                                                # print(todaysDatecode)
                                                try:  # send the email
                                                    mime_message = EmailMessage()  # create an email message object
                                                    # define headers
                                                    mime_message['To'] = toEmail # who the email gets sent to
                                                    mime_message['Subject'] = f'Ineligible Students to participate in {activityProper} for {todaysDatecode}'  # subject line of the email
                                                    mime_message.set_content(f'You are receiving this email because you are a teacher or co-teacher for {activityProper}. The following students are ineligible to participate today because of absences for more than 50% of the day.\nPlease reach out to the athletic department for clarification on any excused absences as they may still be eligible to participate depending on the type of excused absence.\n{absenceString}')  # body of the email
                                                    # encoded message
                                                    encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                                                    create_message = {'raw': encoded_message}
                                                    send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                                                    print(f'DBUG: Email sent, message ID: {send_message["id"]}') # print out resulting message Id
                                                    print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                                                except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                                                    status = er.status_code
                                                    details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                                                    print(f'ERROR {status} from Google API while sending email to {toEmail}: {details["message"]}. Reason: {details["reason"]}')
                                                    print(f'ERROR {status} from Google API while sending email to {toEmail}: {details["message"]}. Reason: {details["reason"]}', file=log)
                                                except Exception as er:
                                                    print(f'ERROR while trying to send email to {toEmail} absences in section {section} of activity {activity}: {er}')
                                                    print(f'ERROR while trying to send email to {toEmail} absences in section {section} of activity {activity}: {er}', file=log)

                                        except Exception as er:
                                            print(f'ERROR while processing student or teacher info to get absences for section {section} of activity {activity}: {er}')
                                            print(f'ERROR while processing student or teacher info to get absences for section {section} of activity {activity}: {er}', file=log)
                                except Exception as er:
                                    print(f'ERROR while processing absences in activity {activity}: {er}')
                                    print(f'ERROR while processing absences in activity {activity}: {er}', file=log)
                        except Exception as er:
                            print(f'ERROR while finding courses with ATH or ACT in their names: {er}')
                            print(f'ERROR while finding courses with ATH or ACT in their names: {er}', file=log)
        endTime = datetime.now()
        endTime = endTime.strftime('%H:%M:%S')
        print(f'INFO: Execution ended at {endTime}')
        print(f'INFO: Execution ended at {endTime}', file=log)