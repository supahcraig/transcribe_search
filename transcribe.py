import youtube_dl
import boto3
import uuid
import time
import requests
import json
import re
from alive_progress import alive_bar

URL = 'your url here'
bucket = 'your bucket name (not the s3 URI, just the name)'

transcription_job_name = str(uuid.uuid4())
audio_track_file = f'{uuid.uuid4()}.mp3'


def extract_audio(URL, audio_track_file):
    #audio_track_file = f'{uuid.uuid4()}.mp3'
    #print(audio_track_file)

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': audio_track_file,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }

    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([URL])


def upload_to_s3(audio_track_file, bucket):
    s3 = boto3.resource(service_name='s3', region_name='us-east-2')
    s3.Object(bucket, audio_track_file).upload_file(Filename={audio_track_file})


def fetch_transcription_URI(transcription_job_name):
    client = boto3.client('transcribe')
    response = client.get_transcription_job(TranscriptionJobName=transcription_job_name)
    return response['TranscriptionJob']['Transcript']['TranscriptFileUri']


def transcribe(bucket, key, transcription_job_name):
    client = boto3.client('transcribe')
    #transcription_job_name = str(uuid.uuid4())

    # the AWS response isn't useful for our purposes
    response = client.start_transcription_job(TranscriptionJobName=transcription_job_name,
                                              LanguageCode='en-US',
                                              Media={'MediaFileUri': f's3://{bucket}/{key}'})


def check_job_status(transcription_job_name):

    def status_check(transcription_job_name):
        response = client.get_transcription_job(TranscriptionJobName=transcription_job_name)
        return response['TranscriptionJob']['TranscriptionJobStatus']

    job_status = status_check(transcription_job_name)

    # transcription jobs are async, so we need to check for job completion
    # alive_bar is a fun progress tracker so you can see that things are happening
    print('waiting on AWS Transcribe.....')
    with alive_bar() as bar:
        while job_status not in ('FAILED', 'COMPLETED'):
            # while loop exit isn't super clean
            time.sleep(1)

            job_status = check_job_status(transcription_job_name)
            bar()

    return job_status


def fetch_words(s3_URI):
    transcription_payload = json.loads(requests.get(s3_URI).text)
    words = transcription_payload['results']['transcripts'][0]['transcript']

    return words


def search_for_words(transcription_dict, search_word_filepath):
    with open(search_word_filepath) as file:
        search_words_list = [line.rstrip().lower() for line in file]

    search_word_usage = {}
    for word in transcription_dict:
        for search_word in search_words_list:
            if search_word in word:
                search_word_usage[word] = transcription_dict[word]

    return search_word_usage


def word_count(transcription_text):
    split_words = transcription_text.lower().split()

    # find word frequency
    wFreq = {}
    for word in split_words:
        # remove punctuation from each word for better grouping (i.e. "hello" vs "hello.")
        scrubbed = re.sub(r'[^\w\s]', '', word)

        try:
            wFreq[scrubbed] += 1
        except KeyError:
            wFreq[scrubbed] = 1

    sortedFreq = {}
    sortedKeys = sorted(wFreq, key=wFreq.get, reverse=True)
    for w in sortedKeys:
        sortedFreq[w] = wFreq[w]

    return sortedFreq


# here is where we run all the steps

# pull the audio & save it as a local file
extract_audio(URL=URL, audio_track_file=audio_track_file)

# upload the audio to s3
upload_to_s3(audio_track_file=audio_track_filename, bucket=bucket)

# start the transcription job
transcribe(bucket=bucket, key=audio_track_file, transcription_job_name=transcription_job_name)

# wait for transcription job to complete
job_status = check_job_status(transcription_job_name=transcription_job_name)

# Get the S3 URI of the transcription output
transcription_URI = fetch_transcription_URI(transcription_job_name=job_name)

# pull the transcript text and turn it into a list of words used
words = fetch_words(s3_URI=transcription_URI)

# turn the list of words into a word/frequency dict (sorted)
wFreq = word_count(transcription_text=words)

# search the transcript for words appearing in our search list
search_word_usage = search_for_words(transcription_dict=wFreq, search_word_filepath='badwords.txt')

# Show the results
print('=' * 50)
print('Entire transcription word frequency, sorted:')
print(wFreq)

print('=' * 50)
print('Identified words & frequency:')
print(f'Found {len(search_word_usage)} identified words used a total of {sum(search_word_usage.values())} times.')
print(search_word_usage)






