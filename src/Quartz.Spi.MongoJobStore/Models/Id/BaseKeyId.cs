﻿namespace Quartz.Spi.MongoJobStore.Models.Id;

internal abstract class BaseKeyId : BaseId
{
    public string Name { get; set; }
    public string Group { get; set; }
}
