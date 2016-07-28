var P = require('bluebird');
var _ = require('lodash');
var azure = require('azure-storage');
var fs = P.promisifyAll(require("fs"));
var path = require('path');

function Container(accountName, accountKey, containerName) {
    this.name = containerName;
    this.service = azure.createBlobService(accountName, accountKey);
}

var listAllHelper = function(container, entries, continuationToken) {

    entries = entries || [];

    return container.listBlobs(continuationToken).then(function(result) {

        // push all the new entries
        Array.prototype.push.apply(entries, result.entries);

        // fetch the next set of entries
        if (result.continuationToken) {
            return listAllHelper(container, entries, result.continuationToken);
        }

        // otherwise return full set of entries
        return entries;

    });
};

Container.prototype.listAllBlobs = function() {
    return listAllHelper(this, [], null);
};

Container.prototype.listBlobs = function(continuationToken) {

    return new P(_.bind(function (resolve, reject) {

        this.service.listBlobsSegmented(this.name, continuationToken,
            function(error, result, response){
                if (error){
                    reject(error);
                } else {
                    resolve(result);
                }
            }
        );
    }, this));
};

Container.prototype.getBlobText = function(blobName) {

    return new P(_.bind(function (resolve, reject) {

        this.service.getBlobToText(this.name, blobName,
            function(error, blobContent, blob){
                if (error){
                    reject(error);
                } else {
                    resolve(blobContent);
                }
            }
        );
    }, this));
};

Container.prototype.getBlobToFile = function(blobName, localFileName, options) {

    return new P(_.bind(function (resolve, reject) {
        this.service.getBlobToLocalFile(this.name, blobName, localFileName, options,
            function(error, blockBlob, response) {
                if (error){
                    reject(error);
                } else {
                    resolve(blockBlob);
                }
            }
        );
    }, this));
};

Container.prototype.getBlobProperties = function (blobName, options) {
    return new P(_.bind(function (resolve, reject) {
        this.service.getBlobProperties(this.name, blobName, options,
            function(error, blob, response) {
                if (error){
                    reject(error);
                } else {
                    resolve(blob);
                }
            }
        );
    }, this));
};

module.exports = Container;