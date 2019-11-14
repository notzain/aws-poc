const KinesisPoller = (kinesis, streamName, console) => {
    const shardIteratorType = 'LATEST';
    const pollFrequency = 1000;

    return {
        poll: function (callback) {
            function describeStream(streamName) {
                const params = {
                    StreamName: streamName,
                };

                return kinesis.describeStream(params).promise();
            }

            function getShardIterators(shardIds) {
                const iterators = shardIds.map(function (shardId) {
                    const params = {
                        ShardId: shardId,
                        ShardIteratorType: shardIteratorType,
                        StreamName: streamName,
                    };

                    return kinesis.getShardIterator(params).promise();
                });

                return Promise.all(iterators);
            }

            function getRecords(shardIteratorIds) {
                const recordSets = shardIteratorIds.map(function (shardIteratorId) {
                    const params = {
                        ShardIterator: shardIteratorId,
                    };

                    return kinesis.getRecords(params).promise();
                });

                return Promise.all(recordSets)
                    .then((recordSets) => update(recordSets))
                    .then((recordSets) => recordSets.map((records) => records.NextShardIterator))
                    .then((shardIteratorIds) => setTimeout(() => getRecords(shardIteratorIds), pollFrequency))
                    .catch(function (err) {
                        console.error(err);
                        setTimeout(() => getRecords(shardIteratorIds), pollFrequency);
                    });
            }

            function update(recordSets) {
                recordSets.forEach(function (records) {
                    records.Records.forEach(function (record) {
                        const status = JSON.parse(record.Data);

                        console.log(status);
                        callback(status);
                    });
                });

                return recordSets;
            }

            return describeStream(streamName)
                .then((data) => data.StreamDescription.Shards.map((shard) => shard.ShardId))
                .then((shardIds) => getShardIterators(shardIds))
                .then((data) => data.map((shardIterator) => shardIterator.ShardIterator))
                .then((shardIteratorIds) => getRecords(shardIteratorIds))
                .catch(console.error);
        }
    };

}