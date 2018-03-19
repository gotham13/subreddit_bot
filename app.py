import ssl
import logging
import time
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Unauthorized
from queue import Queue
from telegram import ReplyKeyboardRemove
from telegram import ReplyKeyboardMarkup
from threading import Thread
import urllib.error
import urllib.parse
import urllib.request
import json
import flood_protection
from telegram import Bot,KeyboardButton,ForceReply,ChatAction
from telegram.ext import Dispatcher, CommandHandler, ConversationHandler, MessageHandler,Updater,Filters,CallbackQueryHandler
from configparser import ConfigParser
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
config = ConfigParser()
config.read('config.ini')
TOKEN = config.get('telegram','bot_token')
mount_point=config.get('openshift','persistent_mount_point')
subreddit_url=config.get('reddit','subreddit_url')
subreddit_name=config.get('reddit','subreddit_name')
adminlist=str(config.get('telegram','admin_chat_id')).split(',')
conn = sqlite3.connect(mount_point+'subreddit_bot.db')
create_table_request_list = [
    'CREATE TABLE subscribers(id TEXT PRIMARY KEY,latest INTEGER DEFAULT 0)',
]
for create_table_request in create_table_request_list:
    try:
        conn.execute(create_table_request)
    except:
        pass
conn.commit()
conn.close()

timeouts = flood_protection.Spam_settings()
# UTILITY FUNCTIONS...............................................................................

# FUNCTION TO ARRANGE INDIVIDUAL ROW OF DATA
def format_message_row(row_list,index,created_time=None):
    row=''
    for item in row_list:
        if not item=='':
            row=row+item+"\n"
    return {'text':str(index)+'. '+row+'\n','length':len(str(index)+'. '+row+'\n'),'created_time':created_time,'index':index}


# FUNCTION TO SEND DATA AND HANDLE PAGINATION
def paginate_and_send(to_send,update=None,bot=None,id=None):
    try:
        tot_len = 0
        message = ''
        for row in to_send:
            if tot_len >= 2500:
                if bot is None:
                    update.message.reply_text(text=message, disable_web_page_preview=True)
                else:
                    bot.send_message(text=message, disable_web_page_preview=True, chat_id=str(id))
                tot_len = 0
                message = ''
            message = message + row['text']
            tot_len = tot_len + row['length']
        if message != '':
            if bot is None:
                update.message.reply_text(text=message, disable_web_page_preview=True)
            else:
                bot.send_message(text=message, disable_web_page_preview=True, chat_id=str(id))
    except Unauthorized:
        conn = sqlite3.connect(mount_point+'subreddit_bot.db')
        c = conn.cursor()
        c.execute("DELETE FROM subscribers WHERE id = (?)", (str(id),))
        conn.commit()
        c.close()
        conn.close()

# FUNCTION TO GET LATEST DATA
def getLatestData(url):
    gcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    url1 = urllib.request.Request(url=url, headers={'Content-Type': 'application/json', 'User-agent': 'Mozilla/5.0'})
    rawData = urllib.request.urlopen(url=url1, context=gcontext).read().decode('utf-8')
    json_data = json.loads(rawData)
    return json_data


# FUNCTION TO EXTRACT USEFUL INFORMATION FROM JSON
def getInformation(to_send,json_data):
    ind = 1
    for information in json_data['data']['children']:
        data = information['data']
        author = 'author: ' + data['author']
        selftext = data['selftext']
        created_time = data['created_utc']
        if (len(selftext) == 0):
            to_send.append(
                format_message_row([data['title'], data['url'], author], index=ind, created_time=created_time))
        else:
            selftext = 'description: \n' + selftext[:400] + "...."
            to_send.append(
                format_message_row([data['title'], data['url'], author, selftext], ind, created_time=created_time))
        ind = ind + 1



sched = BackgroundScheduler()

# FUNCTION TO SEND POST TO SUBSCRIBERS
@sched.scheduled_job('cron',day_of_week='sun')
def subs_sender():
    bot = Bot(TOKEN)
    url = subreddit_url + '/new.json?limit=20'
    json_data = getLatestData(url)
    to_send=[]
    getInformation(to_send=to_send, json_data=json_data)
    conn = sqlite3.connect(mount_point + 'subreddit_bot.db')
    c = conn.cursor()
    c.execute('SELECT * FROM subscribers')
    for row in c.fetchall():
        id=row[0]
        latest_time_temp=row[1]
        latest_time=to_send[0]['created_time']
        new_to_send=[]
        for data in to_send:
            if(data['created_time']>latest_time_temp):
                new_to_send.append(data)
            else:
                break
        c1 = conn.cursor()
        c1.execute('UPDATE subscribers set latest=(?) WHERE id=(?)', (latest_time, id))
        c1.close()
        conn.commit()
        paginate_and_send(bot=bot, to_send=new_to_send, id=id)
        time.sleep(1)
    c.close()
    conn.close()
sched.start()

#..................................................................................................


# COMMAND HANDLER FUNCTION TO SEND TOP 20 POSTS TO USER
@timeouts.wrapper
def new20(bot,update):
    url = subreddit_url+'/new.json?limit=20'
    json_data=getLatestData(url)
    to_send = []
    getInformation(to_send=to_send,json_data=json_data)
    paginate_and_send(to_send,update)

# COMMAND HANDLER FUNCTION TO SUBSCRIBE
@timeouts.wrapper
def subscribe(bot,update):
    conn = sqlite3.connect(mount_point+'subreddit_bot.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO subscribers (id) VALUES (?)",
                  (str(update.message.chat_id),))
    if c.rowcount == 0:
        update.message.reply_text("Already subscribed. Kindly unsubscribe using /unsubscribe and try again")
    else:
        update.message.reply_text("Subscribed :) . I will send new posts from "+subreddit_name+" every week")
    conn.commit()
    c.close()
    conn.close()
    return ConversationHandler.END


# COMMAND HANDLER FUNCTION TO UNSUBSCRIBE
@timeouts.wrapper
def unsubscribe(bot,update):
    conn = sqlite3.connect(mount_point + 'subreddit_bot.db')
    c = conn.cursor()
    c.execute('SELECT id FROM subscribers WHERE id=(?)', (str(update.message.chat_id),))
    if c.fetchone():
        c.execute('DELETE FROM subscribers WHERE id=(?)', (str(update.message.chat_id),))
        conn.commit()
        c.close()
        conn.close()
        update.message.reply_text("Unsubscribed. Use command /subscribe to subscribe again")
    else:
        update.message.reply_text('You are not subscribed. Please use /subscribe command to subscribe')
        c.close()
        conn.close()


# FUNCTION TO LOG ALL KINDS OF ERRORS
def error(bot, update, error):
    logger.warning('Update "%s" caused error "%s"' % (update, error))



@timeouts.wrapper
def start(bot, update):
    update.message.reply_text("HI\nI send new posts from " +subreddit_name+"\nUse command /new to get 20 latest posts\n/subscribe to get new posts every week"
                                  "\nYou can use /cancel any time to cancel operation\nTo see all the commands use /help"
                                  "\n\nORIGINAL CREATOR @gotham13121997\n\nORIGINAL SOURCE CODE \nhttps://github.com/Gotham13121997/subreddit_bot")

@timeouts.wrapper
def help(bot, update):
    update.message.reply_text('/new -> Get 20 latest posts\n'
                              '/subscribe -> Get new posts every week\n'
                              '/unsubscribe -> Unsubscribe from the above\n'
                              '/cancel -> Cancel operation')
@timeouts.wrapper
def cancel(bot, update):
    update.message.reply_text('Cancelled',reply_markup=ReplyKeyboardRemove())


# START OF ADMIN COMMANDS
# START OF ADMIN CONVERSATION HANDLER TO BROADCAST MESSAGE
DB,BDC=range(2)
@timeouts.wrapper
def broadcast(bot,update):
    if not str(update.message.chat_id) in adminlist:
        update.message.reply_text("sorry you are not an admin")
        return ConversationHandler.END
    update.message.reply_text("Send your message")
    return BDC

def broadcast_message(bot,update):
    message = update.message.text
    conn = sqlite3.connect(mount_point + 'subreddit_bot.db')
    c = conn.cursor()
    c.execute('select id from subscribers')
    for row in c.fetchall():
        try:
            bot.send_message(text=message,chat_id=row[0])
        except:
            pass
        time.sleep(1)
    c.close()
    conn.close()
    return ConversationHandler.END
# END OF ADMIN CONVERSATION HANDLER TO BROADCAST MESSAGE


# START OF ADMIN CONVERSATION HANDLER TO REPLACE THE DATABASE
@timeouts.wrapper
def getDb(bot, update):
    if not str(update.message.chat_id) in adminlist:
        update.message.reply_text("sorry you are not an admin")
        return ConversationHandler.END
    update.message.reply_text("send your sqlite database")
    return DB


def db(bot, update):
    file_id = update.message.document.file_id
    newFile = bot.get_file(file_id)
    newFile.download(mount_point+'subreddit_bot.db')
    update.message.reply_text("saved")
    return ConversationHandler.END
# END OF ADMIN CONVERSATION HANDLER TO REPLACE THE DATABASE

@timeouts.wrapper
def givememydb(bot, update):
    if not str(update.message.chat_id) in adminlist:
        update.message.reply_text("sorry you are not an admin")
        return
    bot.send_document(chat_id=update.message.chat_id, document=open(mount_point+'subreddit_bot.db', 'rb'))


# Write your handlers here
def setup(webhook_url=None):
    """If webhook_url is not passed, run with long-polling."""
    logging.basicConfig(level=logging.WARNING)
    if webhook_url:
        bot = Bot(TOKEN)
        update_queue = Queue()
        dp = Dispatcher(bot, update_queue)
    else:
        updater = Updater(TOKEN)
        bot = updater.bot
        dp = updater.dispatcher
        # ADMIN CONVERSATION HANDLER TO BROADCAST MESSAGES
        conv_handler1 = ConversationHandler(
            entry_points=[CommandHandler('broadcast', broadcast)],
            allow_reentry=True,
            states={
                BDC: [MessageHandler(Filters.text, broadcast_message)]
            },

            fallbacks=[CommandHandler('cancel', cancel)]
        )
        # CONVERSATION HANDLER FOR REPLACING SQLITE DATABASE
        conv_handler2 = ConversationHandler(
            entry_points=[CommandHandler('senddb', getDb)],
            allow_reentry=True,
            states={
                DB: [MessageHandler(Filters.document, db)]
            },

            fallbacks=[CommandHandler('cancel', cancel)]
        )
        dp.add_handler(conv_handler1)
        dp.add_handler(conv_handler2)
        dp.add_handler(CommandHandler('new',new20))
        dp.add_handler(CommandHandler('subscribe',subscribe))
        dp.add_handler(CommandHandler('unsubscribe', unsubscribe))
        dp.add_handler(CommandHandler('givememydb', givememydb))
        dp.add_handler(CommandHandler('start', start))
        dp.add_handler(CommandHandler('help', help))

        # log all errors
        dp.add_error_handler(error)
    # Add your handlers here
    if webhook_url:
        bot.set_webhook(webhook_url=webhook_url)
        thread = Thread(target=dp.start, name='dispatcher')
        thread.start()
        return update_queue, bot
    else:
        bot.set_webhook()  # Delete webhook
        updater.start_polling()
        updater.idle()


if __name__ == '__main__':
    setup()