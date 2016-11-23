FROM tomcat:8-jre8

#add dir for uni files
VOLUME /usr/local/uni

#add symlink
RUN rm -rf /usr/local/tomcat/webapps/ROOT
RUN ln -s /usr/local/uni/webapp /usr/local/tomcat/webapps/ROOT
