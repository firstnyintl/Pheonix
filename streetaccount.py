import datetime
import re
import numpy as np
from pandas import HDFStore, Series, DataFrame
import imaplib
import email
import nltk
import BBG
import pdb


def createSAMessage(date, firm, values):
    """
    Creates a pandas Series object with a street account message, calculates additional variables, and stores them in HDF5
    """

    # Create Series Object
    columns = ['Firm', 'Ticker', 'Type', 'Rating', 'PT', 'FX', 'Analyst']
    vals = [firm, values[0], values[1], values[2], values[3], values[4], values[5]]
    vals = [str(i) for i in vals]
    msg = DataFrame([vals], columns=columns, index=[date])

    print '------------------------------'
    print '        NEW MESSAGE           '
    print '------------------------------'
    print 'Date: ' + str(date)
    print 'Firm: ' + firm
    print 'Ticker: ' + values[0]
    print 'Type: ' + values[1]
    print 'Rating: ' + values[2]
    print 'PT: ' + str(values[3])
    print 'FX: ' + values[4]
    print 'Analyst: ' + values[5]
    print ''

    # Create HDF5 interface
    store = HDFStore('streetaccount.h5')
    # store.put('realtime2', msg, format='table', data_columns=True)
    store.append('realtime5', msg, min_itemsize=50, data_columns=True)
    store.close()

    def stHighLow(self):
        """
        Returns string to indicate whether Price Target is street high or low
        """
        if self.PT > np.max(self.ptlist):
            return 'Street High'
        elif self.PT < np.min(self.ptlist):
            return 'Street Low'
        else:
            return 'Neither'

    def quartile(self):
        """
        Returns which quartile the new price target falls under
        """
        ptlist = self.BBGData[self.BBGData['Target Price'] > 0]['Target Price'].values
        high = np.max(ptlist)
        rng = np.ptp(ptlist)
        q = rng / 4.
        if self.PT > high - q:
            return 4
        elif self.PT > high - q * 2:
            return 3
        elif self.PT > high - q * 3:
            return 2
        else:
            return 1

    def pctChange(self):
        """
        Returns the % change between the new and old price targets
        """
        change = self.PT - self.old_PT
        pctDif = change / self.old_PT
        result = pctDif*100
        return result

    # def changeLongTime(self):
    #     """
    #     calculated how long its been since the anayalts last peice and if that length of time is longer than 1 year, True is returned
    #     """
    #     today = self.date
    #     numberofDays = (today - 'weird ass bloomberg date').days
    #     if numberofDays > 360:
    #         return True
    #     else:
    #         return False

    def newAnalyst(self):
        """
        Returns True if there is a new analyst
        """
        return not self.BBGData[self.BBGData['Firm Name'] == self.firm].Analyst.values[0] == self.analyst.upper()

    def shortInt(self):
        """
        Returns True if short interest is above a significant threshold
        """
        shrtInt = BBG.getSingleField(self.ticker, 'SHORT_INT_RATIO')
        return shrtInt > 5


def getMessages():
    """
    Log into email, process unread emails, process events from emails, write events to database
    """

    def emailLogin():
        """
        Log into gmail using IMAP, return imaplib object
        """
        USER = 'ratingsstock'
        PW = 'firstny2015'
        SERVER = 'imap.gmail.com'
        MAIN_FOLDER = 'inbox'
        mail = imaplib.IMAP4_SSL(SERVER)
        mail.login(USER, PW)
        mail.list()
        mail.select(MAIN_FOLDER)
        return mail

    def getLastMsgCore(y, mail):
        """
        Pulls the first unread email and returns a processed core tokenized message
        """
        # Get number of unread emails
        latest_email_uid = y[0].split()[-1]
        result, data = mail.uid('fetch', latest_email_uid, "(RFC822)")
        raw_email = data[0][1]

        # Generate email message object from raw message
        email_message = email.message_from_string(raw_email)

        # Convert to text block
        block = get_text_block(email_message)

        # Get rid of header and footer
        block = block.split('StreetAccount')[1:3]
        block = "".join(block)

        # Get rid of =, /r, /n
        block = block.translate(None, '=\r\\')
        block = block.translate(None, '\n')

        # Tokenize using NLTK
        tokenized_block = nltk.word_tokenize(block)

        # Isolate message from email and return
        index = tokenized_block.index('ET')
        msg = tokenized_block[index-1:]
        return msg

    def getDate(msg):
        """
        Process date from message
        """
        # Sometimes (e.g. in case of revision), chars stuck in date, remove
        sub = re.sub('[^a-zA-Z_]+', '', msg[0])

        # Get Date
        if not sub: dt = msg[2] + ' ' + msg[0]
        else: dt = msg[2] + ' ' + msg[0].replace(sub, '')

        # Store date in datetime object
        date = datetime.datetime.strptime(dt, '%m/%d/%y %H:%M')
        return date

    def getFirm(msg):
        """
        Cross reference with firm database and return firm name
        """
        # Get firm name
        csv_file = 'research_firm_list.csv'
        firm_list = DataFrame.from_csv(csv_file)
        firm_list['eval'] = firm_list['Unique Identifier'].isin(msg)
        try:
            firm = firm_list[firm_list['eval']].index.values[0]
        except:
            firm = ''
        return firm


    def get_text_block(email_message_instance):
        """
        Process raw email string, returns decoded message string
        """
        maintype = email_message_instance.get_content_maintype()
        if maintype == 'multipart':
            for part in email_message_instance.get_payload():
                if part.get_content_maintype() == 'text':
                    return part.get_payload(decode=True)
        elif maintype == 'text':
            return email_message_instance.get_payload(decode=True)

    def getCurrency(PT, msg, PTix):
        """
        Process currecy identifiers for PT subsection of msg, returns currency string
        """
        # If PT field doesnt contain currency identifier
        if PT == msg[PTix]:
            curr = msg[PTix-1]
            # If identifier is $
            if curr == '$':
                # If 'C' precedes $, currency is CAD
                if msg[PTix-2] == 'C':
                    return 'CAD'
                # If 'C' precedes $, currency is CAD
                if msg[PTix-2] == 'A':
                    return 'AUD'
                # If no 'C' precedes $, currency is USD
                else:
                    return 'USD'
            else: return curr
        else:
            # Seperate non digits from PT field
            curr = ''.join(i for i in msg[PTix] if not i.isdigit())
            # Get rid of potential "."
            curr = curr.replace('.', '')
            # Check if 'p' in identifier, return GBP
            if 'p' in curr:
                return 'GBP'
            # Check if Euro sign
            if '\x80' in curr:
                return 'EUR'
            return curr

    def getPT(msg):
        if "Target" in msg: ix = msg.index('Target')
        elif 'target' in msg: ix = msg.index('target')
        else: return '', ''
        for i, x in enumerate(msg[ix:]):
            # Check if contains float
            if len(re.findall('\d+.\d+', x)) > 0:
                PT = re.findall('\d+.\d+', x)[0]
                curr = getCurrency(PT, msg, ix+i)
                PT = float(PT.replace(',', ''))
                break
            # Check if contains int
            elif len(re.findall('\d+', x)) > 0:
                PT = re.findall('\d+', x)[0]
                curr = getCurrency(PT, msg, ix+i)
                PT = float(PT.replace(',', ''))
                break
            # If doesn't contain float or int continue
            else:
                continue
        return PT, curr

    def getSingle(msg):
        """
        Check if single message and, if so, process message and return event values

        Returns a tuple: (boolean, list), where:
        --boolean: "True" if single event, otherwise "False"
        --list: [ticker, msgtype, rating, PT, curr, analyst]
        """

        # If msg contains any of the following, there are multiple events per message
        multiple_msg_tokens = ['upgrades', 'downgrades', 'initiates', 'resumes', 'reinstates', 'assumes', 'notable']

        # If rating starts with one of the following, it is a two-word rating (e.g. "sector perform")
        two_word_rating_list = ['sector', 'market', 'strong']

        # Check whether is single upgrade / downgrade / initiation / resumption
        if not any([e in msg for e in multiple_msg_tokens]):

            # Get ticker
            SINGLE_MESSAGE_TICKER_INDEX = 5
            ticker = msg[SINGLE_MESSAGE_TICKER_INDEX]

            # Get Analyst (assumed that message ends right after analyst or has another bullet)
            if "Analyst" in msg:
                ix = msg.index('Analyst')

                # Find beginning of analyst name
                if msg[ix+1] != 'is': startix = ix+1
                else: startix = ix+2

                # Get full msg substring until next bullet or end of msg, extract analyst
                msg_substring = ' '.join(msg[startix:])
                if '*' in msg_substring: analyst = msg_substring[:msg_substring.index('*')]
                else: analyst = msg_substring

            else:
                analyst = ''

            # Get message type and rating

            # Check if target price increase or decrease
            # Assumed format 'target in/decreased to x' before first bullet or +5 spaces
            try:
                stpindx = msg.index('*')
            except:
                stpindx = 5
            if 'target' in msg[SINGLE_MESSAGE_TICKER_INDEX:stpindx]:
                ix = msg.index('target')
                if msg[ix+1] == 'increased' or msg[ix-1] == 'raises':
                    msgtype = 'PT increased'
                    PT, curr = getPT(msg)
                    rating = ''
                elif msg[ix+1] == 'decreased' or msg[ix-1] == 'lowers':
                    msgtype = 'PT decreased'
                    PT, curr = getPT(msg)
                    rating = ''
                else:
                    return True, None
                return True, [ticker, msgtype, rating, PT, curr, analyst]
            # If not PT change, check for other msg types 
            else:
                key_dict = {'upgraded': 'upgrade',
                            'downgraded': 'downgrade',
                            'initiated': 'initiation',
                            'resumed': 'resumption',
                            'reinstated': 'resumption',
                            're-instated': 'resumption',
                            'reinitiated': 'resumption',
                            're-initiated': 'resumption',
                            'assumed': 'resumption',
                            }
                msgtype_recognized = False
                for key, value in key_dict.iteritems():
                    if key in msg:
                        msgtype_recognized = True
                        msgtype = value
                        ix = msg.index(key)
                        if msg[ix+2] in two_word_rating_list:
                            rating = msg[ix+2] + ' ' + msg[ix+3]
                        else:
                            rating = msg[ix+2]
                # If msg type not accounted for, exit
                if not msgtype_recognized: return True, None

                # Get Price Target and currency
                PT, curr = getPT(msg)
                return True, [ticker, msgtype, rating, PT, curr, analyst]
        return False, None

    def processMsg(msg):
        """
        Process message, if successful, write to database, if not print why
        """
        # Process date
        date = getDate(msg)

        # Process firm name
        firm = getFirm(msg)

        # Check if email only contains one event (one ticker)
        isSingle, vals = getSingle(msg)

        # If contains only one event
        if isSingle:

            # If doesn't recognize type print following
            if vals is None:
                print '------------------------------'
                print '        NEW MESSAGE           '
                print '------------------------------'
                print ' Can\'t process message type  '
                print ''

            # If type recognized, write event to database
            else:
                createSAMessage(date, firm, vals)

        # If more than one event, print following
        else:
            print '------------------------------'
            print '        NEW MESSAGE           '
            print '------------------------------'
            print 'Can\'t process more than one msg'
            print ''

    # Login to gmail and get mail object
    mail = emailLogin()

    # Get unread message ids
    x, y = mail.uid('search', None, "(UNSEEN)")

    # Check if there are unread emails
    unreadMails = len(y[0].split()) > 0

    # While there are unread emails
    while(unreadMails):

        # Get first unread email and get processed message
        msg = getLastMsgCore(y, mail)

        # Process message
        processMsg(msg)

        # Update unread email boolean
        x, y = mail.uid('search', None, "(UNSEEN)")
        unreadMails = len(y[0].split()) > 0

# If this is executed as script, i.e. from console "python streetaccount.py"
if __name__ == '__main__':
    getMessages()
