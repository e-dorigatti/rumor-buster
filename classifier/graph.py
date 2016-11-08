import json
import multiprocessing as mp
import os
import click
import datetime
import sentiment
import pickle
from matplotlib import pyplot as plt


def get_sentiment((input_file, classifier)):
    base_date = datetime.datetime(3000,12,31,12,12,12)
    upper_date = datetime.datetime(1000,01,01,00,00,00)

    sayac = 0
    with open(input_file) as f:
        for row in f:
            tweet = json.loads(row)
            sayac = sayac + 1
                
            temp_date = tweet['_source']['created_at']
                
            temp_date = temp_date.split(" ")
            temp_year = int(temp_date[5])
                
            if temp_date[1] == "Jan":
                temp_month = 1
            if temp_date[1] == "Feb":
                temp_month = 2
            if temp_date[1] == "Mar":
                temp_month = 3
            if temp_date[1] == "Apr":
                temp_month = 4
            if temp_date[1] == "May":
                temp_month = 5
            if temp_date[1] == "Jun":
                temp_month = 6
            if temp_date[1] == "Jul":
                temp_month = 7
            if temp_date[1] == "Aug":
                temp_month = 8
            if temp_date[1] == "Sep":
                temp_month = 9
            if temp_date[1] == "Oct":
                temp_month = 10
            if temp_date[1] == "Nov":
                temp_month = 11
            if temp_date[1] == "Dec":
                temp_month = 12	
                

            temp_day = int(temp_date[2])

            temp_hour = int(temp_date[3][0] + temp_date[3][1])
            temp_min = int(temp_date[3][3] + temp_date[3][4])
            temp_sec = int(temp_date[3][6] + temp_date[3][7]) 

            date = datetime.datetime(temp_year,temp_month,temp_day,temp_hour,temp_min,temp_sec)
            
            if date < base_date:
                base_date = date
            if date > upper_date:
                upper_date = date
            

        print base_date
        print upper_date

        #find number of bins using time difference
        t_diff = upper_date-base_date
        print t_diff
        diff = str(t_diff)		
        
        if diff[2] == 'd':
            if diff[5] == 's':
                if diff[9].isdigit():
                    number_of_bins = int(diff[0])*6 + int(int(diff[8]+diff[9])/4) + 1
                else:
                    number_of_bins = int(diff[0])*6 + int(int(diff[8])/4) + 1	
            else:
                if diff[8].isdigit():
                    number_of_bins = int(diff[0])*6 + int(int(diff[7]+diff[8])/4) + 1
                else:
                    number_of_bins = int(diff[0])*6 + int(int(diff[7])/4) + 1	
            
        elif diff[3] == 'd':
            if diff[10].isdigit():		
                number_of_bins = int(diff[0]+diff[1])*6 + int(int(diff[9]+diff[10])/4) + 1
            else:
                number_of_bins = int(diff[0]+diff[1])*6 + int(int(diff[9])/4) + 1
            
        elif diff[4] == 'd':
            if diff[11].isdigit():
                number_of_bins = int(diff[0]+diff[1]+diff[2])*6 + int(int(diff[10]+diff[11])/4) + 1
            else:
                number_of_bins = int(diff[0]+diff[1]+diff[2])*6 + int(int(diff[10])/4) + 1
            
        elif diff[0] == '0':
            number_of_bins = 1
            
        else:
            number_of_bins = int(int(diff[0]+diff[1])/4) + 1
            
        #print number_of_bins
        #print sayac
            

        #create data structure to hold tweet bins
        tweet_bins = [[] for i in range(number_of_bins)]

        
        #assign tweets to correct bin
    sayac2 = 0
    f.close()
    with open(input_file) as g:
        for row in g:
            tweet = json.loads(row)
            
            #print sayac2

            temp_date = tweet['_source']['created_at']
            #print temp_date
                
            temp_date = temp_date.split(" ")
            temp_year = int(temp_date[5])
                
            if temp_date[1] == "Jan":
                temp_month = 1
            if temp_date[1] == "Feb":
                temp_month = 2
            if temp_date[1] == "Mar":
                temp_month = 3
            if temp_date[1] == "Apr":
                temp_month = 4
            if temp_date[1] == "May":
                temp_month = 5
            if temp_date[1] == "Jun":
                temp_month = 6
            if temp_date[1] == "Jul":
                temp_month = 7
            if temp_date[1] == "Aug":
                temp_month = 8
            if temp_date[1] == "Sep":
                temp_month = 9
            if temp_date[1] == "Oct":
                temp_month = 10
            if temp_date[1] == "Nov":
                temp_month = 11
            if temp_date[1] == "Dec":
                temp_month = 12	
                

            temp_day = int(temp_date[2])

            temp_hour = int(temp_date[3][0] + temp_date[3][1])
            temp_min = int(temp_date[3][3] + temp_date[3][4])
            temp_sec = int(temp_date[3][6] + temp_date[3][7]) 

            date = datetime.datetime(temp_year,temp_month,temp_day,temp_hour,temp_min,temp_sec)
            
            #print date
            count = base_date
            bin_number = -1		
            for i in range(0,number_of_bins):
                if count > date:
                    break

                else:
                    count = count + datetime.timedelta(hours=4)
                    bin_number = bin_number + 1

            #print bin_number
            
            tweet_bins[bin_number].append(tweet)
            
            sayac2 = sayac2 + 1
        


    #evaluate the bins and collect the score
    score_pos = [[] for i in range(number_of_bins)]
    score_neg = [[] for i in range(number_of_bins)]
    tweet_count = 0
    for j in range(0,number_of_bins):
        pos_count = 0
        neg_count = 0		
        for k in range(0, len(tweet_bins[j])):
            #print j
            #print k
            #print neg_count	
            #print pos_count
            
            result = sentiment.classify(classifier, tweet_bins[j][k]['_source']['text'])
            if result == 'negative':
                neg_count = neg_count + 1
            if result == 'positive':
                pos_count = pos_count + 1
            tweet_count = tweet_count + 1		
            
        score_pos[j] = pos_count
        score_neg[j] = neg_count
        print 'bin', j, 'positive', score_pos[j], 'negative', score_neg[j]

    #print score_pos	
    #print score_neg

    return input_file, score_pos, score_neg


def draw(score_pos, score_neg):
    #drawplot
    index = []
    for h in range(0, len(score_pos)):
        index.append(h)

    plt.plot(index,score_pos, 'g', label='positive', linewidth=4.0)
    plt.plot(index,score_neg, 'r', label='negative', linewidth=4.0)
    plt.legend()
    plt.show()


@click.command()
@click.argument('tweets-dir', type=click.Path(file_okay=False))
@click.argument('classifier', type=click.File('r'))
@click.option('--output-file', '-o', type=click.File('w'), default='features.jsonl')
def main(tweets_dir, classifier, output_file):
    classifier = pickle.load(classifier)

    tasks = (
        (os.path.join(tweets_dir, fname), classifier) for fname in os.listdir(tweets_dir)
    )

    pool = mp.Pool()
    try:
        for fname, score_pos, score_neg in pool.imap(get_sentiment, tasks):
            json.dump((fname, score_pos, score_neg), output_file)
            output_file.write('\n')
    finally:
        pool.close()


if __name__ == '__main__':
    main()
