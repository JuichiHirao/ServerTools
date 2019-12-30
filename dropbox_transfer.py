import dropbox
import os
import sys

target_dir = '/tmp'

args = sys.argv
filename = args[1]

f_cred = open('token.txt')
token = f_cred.read()[:-1]
dbx = dropbox.Dropbox(token)

file_path = os.path.join(target_dir, filename)
if not os.path.isfile(file_path):
    print('not exist {}'.format(file_path))
    exit(-1)

print("Attempting to upload...")
# walk return first the current folder that it walk, then tuples of dirs and files not "subdir, dirs, files"
try:
    file_path = file_path
    dest_path = '/{}'.format(filename)

    with open(file_path, 'rb') as f:
        dbx.files_upload(f.read(), dest_path, mute=True)
except Exception as err:
    print("Failed to upload %s\n%s" % (filename, err))

print("Finished upload.")
