#/bin/bash

volume_list=$(docker volume ls)

echo $volume_list

# for volume in $volume_list
# do
#     echo $(docker volume rm ${volume})
# done