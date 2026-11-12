import os
import sys
import re
from jinja2 import Environment, FileSystemLoader

arg = sys.argv
app = arg[1]

includeFiles = []
pubMsgTypes = []
subMsgTypes = []

def toSnakeCase(string):
    return re.sub("(.[A-Z])",lambda x:x.group(1)[0] + "_" +x.group(1)[1],string).lower()

def main():
    with open(app + "/app.cpp", 'r') as m_f:
            arr = m_f.readlines()
            for m_line in arr:
                publisher = re.search(r"\bcreate_publisher\s*<([^>]+)>", m_line)
                if publisher and publisher.group(1) not in pubMsgTypes:
                    pubMsgTypes.append(publisher.group(1))

                subscriber = re.search(r"\bcreate_subscription\s*<([^>]+)>", m_line)
                if subscriber and subscriber.group(1) not in subMsgTypes:
                    subMsgTypes.append(subscriber.group(1))
                    
            pubSubUnion = list(set(pubMsgTypes + subMsgTypes))
            for msgType in pubSubUnion:
                typeArr = msgType.split('::')
                typeArr[2] = toSnakeCase(typeArr[2])
                includeFile = '/'.join(typeArr)
                includeFile = '#include "' + includeFile + '.hpp"'
                includeFiles.append(includeFile)
                
                
    env = Environment(loader=FileSystemLoader('../mros2/mros2_header_generator'))
    template = env.get_template('templates.tpl')
    datatext = template.render({ "includeFiles":includeFiles, "pubMsgTypes":pubMsgTypes, "subMsgTypes":subMsgTypes  })
    with open(os.path.join(app+"/templates.hpp"), "wb") as f:
        f.write(datatext.encode('utf-8'))


if __name__ == "__main__":
    main()
