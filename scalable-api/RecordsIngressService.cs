using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Logging;
using Infrastructure.Identity;
using Service.EMC;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace ProcessingService.Redis.Records
{
    public class RecordsIngressService : IRecordsIngressService
    {
        private readonly IDatabase _DB;
        private readonly ITenant _Tenant;
        private readonly ILog _Log;

        public RecordsIngressService(IDatabase db, ITenant tenant)
        {
            _DB = db;
            _Tenant = tenant;
            _Log = LogManager.GetLogger<RecordsIngressService>();
        }
        private RedisKey Key() => $"{_Tenant.Name.ToLower()}:records";
        private string GroupName() => $"{_Tenant.ID}";
        
        private void AssertRedisFeatures()
        {
            if (!_DB.Multiplexer.GetServer(_DB.Multiplexer.GetEndPoints().First()).Features.Streams)
            {
                throw new NotSupportedException("Please ensure you are running Redis >= v5.0");
            }
        }

        public async Task Submit(IEnumerable<Record> records)
        {
            AssertRedisFeatures();

            var key = Key();
            var groupName = GroupName();
            var keyExists = await _DB.KeyExistsAsync(key);
            if (!keyExists || (await _DB.StreamGroupInfoAsync(Key())).All(z => z.Name != groupName))
            {
                await _DB.StreamCreateConsumerGroupAsync(Key(), GroupName(), "0-0");
            }

            var messages = records.AsParallel().Select(z => new []
            {
                new NameValueEntry("tenant_id", _Tenant.ID.ToString()),
                new NameValueEntry("record_id", z.RecordID),
                new NameValueEntry("record_created", ((DateTimeOffset)z.Created).ToUnixTimeMilliseconds()),
                new NameValueEntry("record_identifier", z.RecordIdentifier),
                new NameValueEntry("record_payload", JsonConvert.SerializeObject(z))
            }).ToArray();
            
            foreach (var message in messages)
            {
                await _DB.StreamAddAsync(key, message);
            }
        }

        public async IAsyncEnumerable<Record> Yield(string consumerName)
        {
            AssertRedisFeatures();
            
            StreamEntry[] pendingMessages;
            var key = Key();
            var groupName = GroupName();
            var group = (await _DB.StreamGroupInfoAsync(Key())).SingleOrDefault(z => z.Name == groupName);
            RedisValue? position = null;
                      
            do
            {
                pendingMessages = await _DB.StreamReadGroupAsync(key, groupName, consumerName, position, count: 20);
                foreach (var entry in pendingMessages)
                {
                    var payload = entry.Values.Single(z => z.Name == "record_payload");
                    var record = JsonConvert.DeserializeObject<Record>(payload.Value);
                    record.IngressId = entry.Id.ToString();
                    yield return record;
                    position = entry.Id;
                }
            }
            while (pendingMessages.Length > 0);
        }

        public async Task Acknowledge(string consumerName, params string[] ids)
        {
            if (ids.Length == 0)
            {
                _Log.Warn($"{nameof(Acknowledge)} called with 0 {nameof(ids)}.");
                return;
            }
            await _DB.StreamAcknowledgeAsync(Key(), GroupName(), ids.Select(z => (RedisValue)z).ToArray());
        }
    }
}