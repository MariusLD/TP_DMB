import json
import csv

input = {"user_id": "76561197970982479", "user_url": "http://steamcommunity.com/profiles/76561197970982479", "reviews": [{"funny": "", "posted": "Posted November 5, 2011.", "last_edited": "", "item_id": "1250", "helpful": "No ratings yet", "recommend": "True", "review": "Simple yet with great replayability. In my opinion does \"zombie\" hordes and team work better than left 4 dead plus has a global leveling system. Alot of down to earth \"zombie\" splattering fun for the whole family. Amazed this sort of FPS is so rare."}, {"funny": "", "posted": "Posted July 15, 2011.", "last_edited": "", "item_id": "22200", "helpful": "No ratings yet", "recommend": "True", "review": "It\"s unique and worth a playthrough."}, {"funny": "", "posted": "Posted April 21, 2011.", "last_edited": "", "item_id": "43110", "helpful": "No ratings yet", "recommend": "True", "review": "Great atmosphere. The gunplay can be a bit chunky at times but at the end of the day this game is definitely worth it and I hope they do a sequel...so buy the game so I get a sequel!"}]}

# Review n°1 => "review": "Simple yet with great replayability. In my opinion does 'zombie' hordes and team work better than left 4 dead plus has a global leveling system. Alot of down to earth 'zombie' splattering fun for the whole family. Amazed this sort of FPS is so rare.
# Review n°2 => "review": "It's unique and worth a playthrough."
# Review n°3 => "review": "Great atmosphere. The gunplay can be a bit chunky at times but at the end of the day this game is definitely worth it and I hope they do a sequel...so buy the game so I get a sequel!"

# Convert input as a string, replace quotes
def replace_quotes(input):
    return str(input).replace("'", '"')

# For a string, replace every doubles quotes by singles quotes between "review": " and next "}
def replace_quotes_in_review(input):
    index = input.find('"review": "')
    while index != -1:
        index += len('"review": "')
        index2 = input.find('"}', index)
        if index2 != -1:
            input = input[:index] + input[index:index2].replace('"', "'") + input[index2:]
            index = input.find('"review": "', index + 1)
        else:
            break
    return input

# For a string, put doubles quotes for the content after "recommend": and before next ,
def put_double_quotes_in_recommend(input):
    index = input.find('"recommend": ')
    while index != -1:
        index2 = input.find(',', index)
        if index2 != -1:
            input = input[:index] + input[index:index2].replace(':', ':"').strip() + '"' + input[index2:]
            input = input.replace('" ', '"')
            index = input.find('"recommend": ', index + 1)
        else:
            break
    return input

def read_line(input_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        return file.readline()

def replace_quotes_and_save(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        with open(output_file, 'w', encoding='utf-8') as file2:
            for line in file:
                file2.write(put_double_quotes_in_recommend(replace_quotes_in_review(replace_quotes(line))))

def json_to_csv(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        with open(output_file, 'w', encoding='utf-8', newline='') as file2:
            csv_file = csv.writer(file2)
            headers_written = False
            for line in file:
                line = line.replace("\\", "\\\\")
                data = json.loads(line)
                if not headers_written:
                    csv_file.writerow(data.keys())
                    headers_written = True
                csv_file.writerow(data.values())

def main():
    replace_quotes_and_save('australian_user_reviews.json', 'australian_user_reviewsV2.json')
    json_to_csv('australian_user_reviewsV2.json', 'australian_user_reviewsV2.csv')

if __name__ == "__main__":
    main()