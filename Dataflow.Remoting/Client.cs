using System;
using Dataflow.Serialization;

namespace Dataflow.Remoting
{
    public class ServiceMethod
    {
        public string Name { get; protected set; }
        // factories for Params and Result messages.
        public Message Params { get; private set; }
        public Message Result { get; private set; }

        #region class constructors + requests creators

        protected ServiceMethod() { }

        public ServiceMethod(Message args, Message rets, string name)
        {
            Params = args;
            Result = rets;
            Name = name;
        }

        public ServiceMethod(ServiceMethod prototype)
        {
            Params = prototype.Params;
            Result = prototype.Result;
            Name = prototype.Name;
        }

        public ServiceMethod(string name, MessageDescriptor args, MessageDescriptor rets)
        {
            Name = name;
            Params = args.Factory;
            Result = rets.Factory;
        }

        public Request CreateRequest(IRequestTracker tracker)
        {
            return new Request(tracker, Params.New(), Result.New(), this);
        }

        public Request CreateRequest(IRequestTracker tracker, Message args, Message rets)
        {
            return new Request(tracker, args, rets, this);
        }

        internal void CheckCompatible(ServiceMethod sign)
        {
            if (Params == sign.Params || Result == sign.Result)
                return;
            throw new InvalidCastException(sign.Name);
        }

        #endregion
    }

    public class ServiceDefinition
    {
        public ServiceMethod[] Methods { get; private set; }
        public string Name { get; set; }
    }

    public class ServiceClient
    {
        private IChannelSync _sync_;
        private ServiceMethod[] _vmt;

        public IChannel Channel { get; private set; }
        public ServiceDefinition Definition { get; private set; }

        protected IChannelSync ChannelSync { get { return _sync_ ?? (_sync_= Channel as IChannelSync) ?? (_sync_= new ChannelSync(Channel)); } }

        protected ServiceClient(IChannel target, ServiceDefinition definition)
        {
            Channel = target;
            Definition = definition;
            _vmt = definition.Methods;
        }
    }

    public class ServiceProxy
    {
        public ServiceMethod[] Methods { get; private set; }

        protected ServiceProxy(params ServiceMethod[] methods)
        {
            Methods = methods;
        }

        public bool Dispatch(Request task)
        {
            return false;
        }
    }

    public class ServiceMethod<TP, TR, TC> : ServiceMethod
        where TP : Message
        where TR : Message
        where TC : Message
    {
        public Action<TP, TR, TC> Dispatch { get; private set; }

        public ServiceMethod(Action<TP, TR, TC> action, ServiceMethod proto)
            : base(proto)
        {
            if (action == null) throw new ArgumentNullException("action");
            Dispatch = action;
        }

        public void ExecuteRequest(Request task)
        {
            var args = task.Params as TP;
            var rets = task.Result as TR;
            if (args != null && rets != null)
                try
                {
                    Dispatch(args, rets, task.Context as TC);
                }
                catch (Exception exception)
                {
                    task.Fail(new Signal.Runtime(exception));
                }
            else task.Fail(new Signal(Signal.iBadLpc));
        }
    }
}
