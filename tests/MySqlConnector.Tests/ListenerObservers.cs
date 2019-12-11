using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;

namespace MySqlConnector.Tests
{
	public partial class DiagnosticTests
	{
		private class ListenerObservers : IObserver<DiagnosticListener>, IDisposable
		{
			public ConcurrentQueue<(string name, KeyValuePair<string, object>)> Events { get; } = new ConcurrentQueue<(string name, KeyValuePair<string, object>)>();
			private readonly ConcurrentQueue<IDisposable> m_subscriptions = new ConcurrentQueue<IDisposable>();
			public void OnCompleted()
			{
			}

			class EventObserver: IObserver<KeyValuePair<string, object>>
			{
				private readonly ListenerObservers m_parent;
				private readonly string m_streamName;

				public EventObserver(ListenerObservers parent, string streamName)
				{
					m_parent = parent;
					m_streamName = streamName;
				}


				public void OnCompleted()
				{
				}

				public void OnError(Exception error)
				{
				}

				public void OnNext(KeyValuePair<string, object> value)
				{
					m_parent.Events.Enqueue((m_streamName, value));
				}
			}

			public void OnError(Exception error)
			{
			}

			public void OnNext(DiagnosticListener value) => m_subscriptions.Enqueue(value.Subscribe(new EventObserver(this, value.Name)));

			public void Dispose()
			{
				foreach (var subscription in m_subscriptions)
				{
					subscription.Dispose();
				}
			}
		}
	}
}
