import numpy as np
import pandas as pd
import pprint
from bokeh.plotting import figure, show, output_file
from bokeh.palettes import brewer

from cassandra.cluster import Cluster


def resample_sentiment_polarity(source_df, dest_df, sentiment, dateTimeRange, timeDelta):
    """
    sentiment can be, 'polarity_negative','polarity_positive', and 'polarity_neutral' 
    timeDelta can be, pd.Timedelta('1Min') ...etc
    """
    for dateTime in dateTimeRange:
        data_interval = source_df.between_time(dateTime.time(), (dateTime+timeDelta).time())
        data_interval_true = data_interval[data_interval[sentiment+'_boolean']]
        if(data_interval_true.shape[0] != 0):
            data_avg = data_interval_true['sentiment_polarity'].sum()/data_interval_true.shape[0]
            dest_df.loc[dateTime,sentiment] = data_avg
        else:
            dest_df.loc[dateTime,sentiment] = 0

    return dest_df

def resample_sentiment_subjectivity(source_df, dest_df, sentiment, dateTimeRange, timeDelta):
    """
    sentiment can be, 'polarity_negative','polarity_positive', and 'polarity_neutral' 
    timeDelta can be, pd.Timedelta('1Min') ...etc
    """
    for dateTime in dateTimeRange:
        data_interval = source_df.between_time(dateTime.time(), (dateTime+timeDelta).time())
        if(data_interval.shape[0] != 0):
            data_avg = data_interval['sentiment_subjectivity'].sum()/data_interval.shape[0]
            dest_df.loc[dateTime,sentiment] = data_avg
        else:
            dest_df.loc[dateTime,sentiment] = 0

    return dest_df


def preprocess(source_df):
    source_df['createdat'] = pd.to_datetime(source_df['createdat'])
    source_df = source_df.set_index('createdat')
    #df.resample('1Min', how='mean')

    source_df['polarity_negative_boolean'] = source_df['sentiment_polarity'] < 0
    source_df['polarity_positive_boolean'] = source_df['sentiment_polarity'] > 0
    source_df['polarity_neutral_boolean'] = source_df['sentiment_polarity'] == 0

    timeFrequency = '1Min'
    dateTimeRange = pd.date_range(start = source_df.index.min(axis=1), end = source_df.index.max(axis=1), freq=timeFrequency)
    dateTimeDelta = pd.Timedelta(timeFrequency)
    #newDf = 
    #for dateTime in dateTimeRange:
    newDf = pd.DataFrame({'time': dateTimeRange})
    newDf = newDf.set_index('time')

    newDf = resample_sentiment_polarity(source_df, newDf, 'polarity_negative', dateTimeRange, dateTimeDelta)
    newDf = resample_sentiment_polarity(source_df, newDf, 'polarity_positive', dateTimeRange, dateTimeDelta)
    newDf = resample_sentiment_polarity(source_df, newDf, 'polarity_neutral', dateTimeRange, dateTimeDelta)
    newDf = resample_sentiment_subjectivity(source_df, newDf, 'subjectivity_all', dateTimeRange, dateTimeDelta)
    resample_sentiment_subjectivity

    return newDf

cluster = Cluster()
session = cluster.connect()
session.set_keyspace("twitter_sentiment")

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames) 

session.row_factory = pandas_factory
session.default_fetch_size = None

a = session.execute("select * from twitter_sentiment_table")
df = a._current_rows
#df.to_pickle('trump_data.pkl')
#df = pd.read_pickle('sent_data.pkl')

newDf = preprocess(df)

unstackedDf = newDf.unstack()
stringCSV = unstackedDf.to_csv()
stringCSV = "sentiment_cat,time,polarity\n"+stringCSV
newDf2 = pd.read_csv(pd.compat.StringIO(stringCSV), index_col=0) #nice hack


#ALTERNATIVE 1
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show, save, output_file

p = figure(x_axis_type="datetime", plot_width=1000, plot_height=500)
from bokeh.models.formatters import DatetimeTickFormatter
p.xaxis.formatter=DatetimeTickFormatter(
        seconds=["%H:%M"],
        minutes=["%H:%M"],
        hours=["%F %H:%M"],
        days=["%F %H:%M"],
        months=["%F %H:%M"],
        years=["%F %H:%M"],
    )


p.line('time', 'polarity_positive', source=newDf, line_color = 'green', legend='Positive Sentiment Polarity(0 to 1)')
p.line('time', 'polarity_negative', source=newDf, line_color = 'red', legend='Negative Sentiment Polarity(0 to -1)')
p.line('time', 'subjectivity_all', source=newDf, line_color = 'blue', legend='Subjectivity Score(0 to 1)')
p.line('time', 'polarity_neutral', source=newDf, line_color = 'black')
p.yaxis.axis_label = 'Average Sentiment Polarity & Subjectivity'
p.xaxis.axis_label = 'Time'
p.title.text = "Sentiment Over Time"
p.title.align = "center"
p.legend.location = "center"

show(p)#this will also output a HTML file, so that the plot can be saved.
#save(p)