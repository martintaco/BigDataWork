import os

json_filename = 'ds_mailgrgz_20191126.json'
LOCAL_WORK_DIR = '/home/belcorpuser/jtaco/WorkPlace/brackets'
remove_last_comma = "'$s/,$//'"
add_bracket_beginning = "'1s/^/[\\n/'"
add_bracket_ending = '"' + "\$a\]" + '"'

remove_last_comma_command = "sed -i " + remove_last_comma + " " + LOCAL_WORK_DIR + "/" + json_filename
os.system(remove_last_comma_command)
print('The last comma was removed')

add_bracket_beginning_command = "sed -i " + add_bracket_beginning + " " + LOCAL_WORK_DIR + "/" + json_filename
print('Add square brackets at the beginning')
os.system(add_bracket_beginning_command)

add_bracket_ending_command = "sed -i " + add_bracket_ending + " " + LOCAL_WORK_DIR + "/" + json_filename
print('Add square brackets at the ending')
os.system(add_bracket_ending_command)

print("Ya puedes revisar tu archivo")