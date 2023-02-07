#! /bin/sh

# declaring a variable, and assigning it to
# value of the first parameter the script receive
repoName=$1

# create a conditional to keep asking the user to enter
# the repo name

# While the repoName variable is not assigned
while [ -z "$repoName" ]
do
    # Write to the console this message
    echo 'Provide a repository name'
    # Read whatever input the user provides
    # assign the input to the repoName variable
    read -r -p $'repository name:' repoName
done

# create readme file and writting a single line with the repo name
echo "# $repoName" >> README.md

git init
git add .
git commit -m 'First commit'

# curl is a command to transfer data from or to server
# -u flag to declare the user we are creating the repo for
# -d flag to pass paramaters to the command

curl -u acothaha https://api.github.com/user/repos -d '{"name": "'"$repoName"'", "private":false}'


# -H flag sets the header of the request
# Piping using | symbol, meaning passing the return value of a process as the input value of another process
# jq command, a tool for processing JSON inputs

GIT_URL=$(curl -H "Accept: application/vnd.github.v3+json" https://api.github.com/repos/acothaha/"$repoName" | jq -r '.clone_url')

git branch -M main
git remote add origin $GIT_URL
git push -u origin main