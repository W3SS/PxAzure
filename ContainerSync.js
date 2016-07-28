var P = require('bluebird');
var _ = require('lodash');
var azure = require('azure-storage');
var fs = P.promisifyAll(require("fs"));
var path = require('path');
var mkdirp = require('mkdirp');

function ContainerSync(container, destinationPath, options) {
    this.container = container;
    this.destinationPath = destinationPath;

    var defaults = {

        // refresh interval in ms.
        interval: 5 * 60 * 1000,

        // remove local files that no longer exist in container?
        deleteOrphans: false,

        // switches for logging
        log: {
            start: true,
            download: true,
            skip: false,
            delete: true
        },

        autoStart: false,
    };

    this.options = _.extend(defaults, options);
    mkdirp.sync(this.destinationPath);

    this.lastRunTime = 0;
    this.started = false;
    this.timeoutId = null;
    if (this.options.autoStart) {
        this.start();
    }
}

ContainerSync.prototype.start = function() {
    if (!this.started) {
        this.started = true;
        this.run();
    }
};

ContainerSync.prototype.stop = function() {
    if (this.started) {
        clearTimeout(this.timeoutId);
        this.started = false;
    }
};

var syncEntry = function(sync, entry) {

    var blobName = entry.name;
    var azureLastModified = new Date(entry.lastModified);
    var destinationFilename = path.join(sync.destinationPath, blobName);

    return fs.statAsync(destinationFilename).then(function(stats) {

            // see if the last modified time stamps match
            var localLastModified = new Date(stats.mtime);
            if (localLastModified.getTime() === azureLastModified.getTime()) {

                if (sync.options.log.skip) {
                    console.log('skipping download of ' + blobName + ' with same last modified');
                }

                return false;
            }

            return true;
        })
        .catch(function(err) {
            // error occurs if file doesn't exist, so we need to download
            return true;
        })
        .then(function(shouldDownload) {
            if (!shouldDownload) {
                return destinationFilename;
            }

            // ensure the target directory exists
            var lastSlashIndex = destinationFilename.lastIndexOf('/');
            var destinationDirectory = destinationFilename.substring(0, lastSlashIndex + 1);
            mkdirp.sync(destinationDirectory);

            if (sync.options.log.download) {
                console.log('start download of ' + blobName);
            }

            return sync.container.getBlobToFile(blobName, destinationFilename)
                .then(function(blob) {

                    if (sync.options.log.download) {
                        console.log('completed download of ' + blobName);
                    }

                    // we need to set last modified time to match azure
                    fs.utimesSync(destinationFilename, new Date(), azureLastModified);
                    return destinationFilename;
                });
        });
};

var removeOrphans = function(path, keepFiles, logDelete) {
    return fs.statAsync(path)
        .then(function(stats) {
            if (stats.isDirectory()) {
                return;
            }

            // TODO: implement delete
        });
};

ContainerSync.prototype.ensureHasRun = function() {
    if (this.lastRunTime > 0) {
        return P.resolve();
    }

    return this.run();
};


ContainerSync.prototype.run = function() {

    // ensure we only have one sync running at a time
    if (!this.currentRun) {

        var sync = this;
        this.currentRun = this.container.listAllBlobs()
            .then(function(blobEntries) {

                if (sync.options.log.start) {
                    console.log('Sync ' + blobEntries.length + ' blobs from ' +
                        sync.container.name + ' to ' + sync.destinationPath);
                }

                // download each of the blobs
                var promises = _.map(blobEntries, function(entry) { return syncEntry(sync, entry); });
                return P.all(promises);
            })
            .then(function(destinationFilenames) {

                // delete orphaned files
                if (sync.options.deleteOrphans) {
                    return removeOrphans(
                        sync.destinationPath, 
                        destinationFilenames, 
                        sync.options.log.delete);
                }
                   
                //console.log(destinationFilenames);
            })
            .then(function() {

                this.lastRunTime = Date.now();
                this.currentRun = null;

                // schedule next run
                if (sync.started) {
                    sync.timeoutId = setTimeout(function() {
                        sync.run()
                            .catch(function(ex) {
                                console.log('Exception on background container sync: ' + ex);
                            });
                    }, sync.options.interval);
                }

            });
    }

    return this.currentRun;
};


module.exports = ContainerSync;
