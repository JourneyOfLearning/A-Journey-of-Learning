using Autofac;
using Autofac.Core.Lifetime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;
using Common.Logging;

namespace RecordProcessingService
{
    // IAsyncRunner is registered as a singleton
    public interface IAsyncRunner
    {
        void ProcessRecords(string tenant, string user);
    }

    public class AsyncRunner : IAsyncRunner
    {
        private ILifetimeScope _Scope { get; set; }

        private Dictionary<string, int> _CurrentProcessing = new Dictionary<string, int>();
        private readonly object processingLock = new object();

        private readonly int MAX_WORKERS = 20;

        public AsyncRunner(ILifetimeScope scope)
        {
            _Scope = scope;
        }

        public void ProcessRecords(string tenant, string user)
        {
            lock (processingLock)
            {
                var currentProcessingCounter = _CurrentProcessing[tenant];

                if ( currentProcessingCounter == MAX_WORKERS)
                {
                    return;
                }
                else
                {
                    _CurrentProcessing[tenant]++;
                }
            }

            HostingEnvironment.QueueBackgroundWorkItem(ct =>
            {
                try
                {
                    using (var scope = _Scope.BeginLifetimeScope(MvcModule.AsyncRunnerLifetimeScopeTag, builder =>
                    {
                        builder.RegisterType<AsyncSessionState>().As<ISessionState>().WithParameter("user", user);
                    }))
                    {
                        var records = scope.Resolve<IRecordService>();

                        records.ProcessIngress(tenant);
                    }

                    lock (processingLock)
                    {
                        _CurrentProcessing[tenant]--;
                    }
                }
                catch (Exception e)
                {
                    lock (processingLock)
                    {
                        _CurrentProcessing[tenant]--;
                    }

                    _Log.Error(e);

                    _SendEmailToSupport();
                }
            });
        }
    }
}