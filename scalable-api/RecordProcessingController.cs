using Common;
using Models.Common;
using Models.Records;
using Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http.Formatting;
using System.Web.Http;


namespace Controllers.Api
{

	[Authorize(Roles = Roles.User)]
	public class RecordProcessingController : ApiController
	{
		private readonly IRecordService _Records;
		private readonly ISessionState _Session;
		private readonly ITenant _Tenant;
		private readonly IAsyncRunner _Runner;

		public RecordProcessingController(IRecordService records, ISessionState session, IAsyncRunner runner, ITenant tenant)
		{
			_Session = session;
			_Tenant = tenant;
			_Records = records;
			_Runner = runner;
		}

		[HttpPost]
		[Route("api/items/import")]
		public void Post([FromBody] IEnumerable<Record> records) 
		{
			_Records.Submit(records);

			var user = _Session.User;
			var tenant = _Tenant.Name;
			_Runner.ProcessRecords(tenant, user);
		}

	}
}